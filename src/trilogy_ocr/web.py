"""Flask web UI for running the Trilogy OCR pipeline."""

from __future__ import annotations

import os
import threading
import time
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from flask import Flask, abort, jsonify, redirect, render_template, request, send_file, url_for
from werkzeug.utils import secure_filename

from . import pipeline

BASE_DIR = Path(__file__).resolve().parents[2]
RUNS_DIR = BASE_DIR / "web_runs"
PERSISTENT_CHECKS_DIR = BASE_DIR / "checks"
RUNS_DIR.mkdir(parents=True, exist_ok=True)
PERSISTENT_CHECKS_DIR.mkdir(parents=True, exist_ok=True)

JOBS: dict[str, dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()


def _build_job_snapshot(job: dict[str, Any]) -> dict[str, Any]:
    now = time.time()
    start_ts = float(job.get("start_ts") or job["created_ts"])
    end_ts = float(job.get("end_ts") or now)
    elapsed_seconds = max(0.0, end_ts - start_ts)

    processed_pages = int(job.get("processed_pages", 0))
    total_pages = int(job.get("total_pages", 0))
    eta_seconds: float | None = None
    if total_pages > 0 and processed_pages > 0 and processed_pages < total_pages:
        rate = elapsed_seconds / float(processed_pages)
        eta_seconds = max(0.0, rate * (total_pages - processed_pages))

    output_csv = Path(job["output_csv"])
    return {
        "job_id": job["job_id"],
        "created_at": datetime.fromtimestamp(job["created_ts"]).strftime("%Y-%m-%d %H:%M:%S"),
        "input_count": int(job["input_count"]),
        "status": str(job.get("status", "queued")),
        "rows_written": int(job.get("rows_written", 0)),
        "processed_pages": processed_pages,
        "total_pages": total_pages,
        "current_file": str(job.get("current_file", "")),
        "error": str(job.get("error", "")),
        "elapsed_seconds": round(elapsed_seconds, 1),
        "eta_seconds": round(eta_seconds, 1) if eta_seconds is not None else None,
        "progress_percent": round((processed_pages / total_pages) * 100, 1) if total_pages else 0.0,
        "download_url": f"/download/{job['job_id']}" if output_csv.exists() else "",
    }


def _run_job(job_id: str, checks_dir: Path, output_csv: Path, api_key: str) -> None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return
        job["status"] = "running"
        job["start_ts"] = time.time()

    if api_key:
        os.environ["MISTRAL_API_KEY"] = api_key
        pipeline.MISTRAL_API_KEY = api_key

    def progress_update(update: dict[str, Any]) -> None:
        with JOBS_LOCK:
            current = JOBS.get(job_id)
            if not current:
                return
            current["status"] = str(update.get("state") or current.get("status") or "running")
            current["rows_written"] = int(update.get("rows_written", current.get("rows_written", 0)))
            current["processed_pages"] = int(update.get("processed_pages", current.get("processed_pages", 0)))
            current["total_pages"] = int(update.get("total_pages", current.get("total_pages", 0)))
            current["current_file"] = str(update.get("current_file", current.get("current_file", "")))

    try:
        rows_written = pipeline.process_checks_to_csv(
            str(checks_dir),
            str(output_csv),
            progress_callback=progress_update,
        )
        with JOBS_LOCK:
            current = JOBS.get(job_id)
            if current:
                current["status"] = "completed"
                current["rows_written"] = rows_written
                current["processed_pages"] = int(current.get("total_pages", current.get("processed_pages", 0)))
                current["end_ts"] = time.time()
    except Exception as exc:  # pragma: no cover - runtime surface
        with JOBS_LOCK:
            current = JOBS.get(job_id)
            if current:
                current["status"] = "failed"
                current["error"] = f"{exc}\n\n{traceback.format_exc()}"
                current["end_ts"] = time.time()


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024

    @app.get("/")
    def index() -> str:
        return render_template("index.html")

    @app.post("/run")
    def run_pipeline() -> str:
        uploaded_files = request.files.getlist("pdf_files")
        pdf_files = [f for f in uploaded_files if f and f.filename and f.filename.lower().endswith(".pdf")]

        if not pdf_files:
            return render_template("index.html", error="Upload at least one PDF file.")

        api_key = (request.form.get("api_key") or "").strip()
        if api_key:
            os.environ["MISTRAL_API_KEY"] = api_key
            pipeline.MISTRAL_API_KEY = api_key

        job_id = uuid.uuid4().hex[:12]
        run_dir = RUNS_DIR / job_id
        checks_dir = run_dir / "checks"
        output_dir = run_dir / "output"
        checks_dir.mkdir(parents=True, exist_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)

        for file in pdf_files:
            safe_name = secure_filename(file.filename)
            if not safe_name.lower().endswith(".pdf"):
                continue
            file_bytes = file.read()
            if not file_bytes:
                continue
            (checks_dir / safe_name).write_bytes(file_bytes)
            (PERSISTENT_CHECKS_DIR / safe_name).write_bytes(file_bytes)

        output_csv = output_dir / "royalty_checks.csv"
        with JOBS_LOCK:
            JOBS[job_id] = {
                "job_id": job_id,
                "created_ts": time.time(),
                "start_ts": None,
                "end_ts": None,
                "status": "queued",
                "input_count": len(pdf_files),
                "rows_written": 0,
                "processed_pages": 0,
                "total_pages": 0,
                "current_file": "",
                "error": "",
                "output_csv": str(output_csv),
            }

        worker = threading.Thread(target=_run_job, args=(job_id, checks_dir, output_csv, api_key), daemon=True)
        worker.start()

        return redirect(url_for("job_page", job_id=job_id))

    @app.get("/job/<job_id>")
    def job_page(job_id: str) -> str:
        with JOBS_LOCK:
            job = JOBS.get(secure_filename(job_id))
            if not job:
                abort(404)
            snapshot = _build_job_snapshot(job)
        return render_template("result.html", result=snapshot)

    @app.get("/status/<job_id>")
    def job_status(job_id: str) -> Any:
        with JOBS_LOCK:
            job = JOBS.get(secure_filename(job_id))
            if not job:
                abort(404)
            snapshot = _build_job_snapshot(job)
        return jsonify(snapshot)

    @app.get("/download/<job_id>")
    def download_csv(job_id: str) -> Any:
        target = RUNS_DIR / secure_filename(job_id) / "output" / "royalty_checks.csv"
        if not target.exists():
            abort(404)
        return send_file(target, as_attachment=True, download_name=f"royalty_checks_{job_id}.csv")

    return app


def main() -> None:
    app = create_app()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=False)


if __name__ == "__main__":
    main()

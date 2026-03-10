"""Flask web UI for running the Trilogy OCR pipeline."""

from __future__ import annotations

import csv
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
        "preview_url": f"/preview/{job['job_id']}" if output_csv.exists() else "",
        "csv_name": output_csv.name,
        "can_cancel": str(job.get("status", "")) in {"queued", "running", "cancelling"},
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

    def should_stop() -> bool:
        with JOBS_LOCK:
            current = JOBS.get(job_id)
            if not current:
                return True
            return bool(current.get("cancellation_requested", False))

    try:
        rows_written = pipeline.process_checks_to_csv(
            str(checks_dir),
            str(output_csv),
            progress_callback=progress_update,
            should_stop=should_stop,
        )
        with JOBS_LOCK:
            current = JOBS.get(job_id)
            if current:
                if current.get("cancellation_requested") or current.get("status") == "cancelled":
                    current["status"] = "cancelled"
                else:
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

    def find_brand_pdf() -> str:
        search_roots = [BASE_DIR, PERSISTENT_CHECKS_DIR]
        for root in search_roots:
            for path in sorted(root.glob("**/*trilogy*.pdf")):
                if path.is_file():
                    try:
                        return str(path.relative_to(BASE_DIR))
                    except ValueError:
                        continue
        return ""

    def find_brand_image() -> str:
        candidates = [
            BASE_DIR / "Trilogy.jpg",
            BASE_DIR / "trilogy.jpg",
            BASE_DIR / "Trilogy.png",
            BASE_DIR / "trilogy.png",
        ]
        for path in candidates:
            if path.exists() and path.is_file():
                try:
                    return str(path.relative_to(BASE_DIR))
                except ValueError:
                    continue
        return ""

    @app.get("/")
    def index() -> str:
        return render_template("index.html", brand_pdf=find_brand_pdf(), brand_image=find_brand_image())

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
                "cancellation_requested": False,
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
        return render_template("result.html", result=snapshot, brand_image=find_brand_image())

    @app.get("/status/<job_id>")
    def job_status(job_id: str) -> Any:
        with JOBS_LOCK:
            job = JOBS.get(secure_filename(job_id))
            if not job:
                abort(404)
            snapshot = _build_job_snapshot(job)
        return jsonify(snapshot)

    @app.post("/cancel/<job_id>")
    def cancel_job(job_id: str) -> Any:
        safe_id = secure_filename(job_id)
        with JOBS_LOCK:
            job = JOBS.get(safe_id)
            if not job:
                abort(404)
            status = str(job.get("status", ""))
            if status in {"completed", "failed", "cancelled"}:
                return jsonify({"ok": True, "status": status})
            job["cancellation_requested"] = True
            if status in {"queued", "running"}:
                job["status"] = "cancelling"
            snapshot = _build_job_snapshot(job)
        return jsonify({"ok": True, "status": snapshot["status"]})

    @app.get("/download/<job_id>")
    def download_csv(job_id: str) -> Any:
        target = RUNS_DIR / secure_filename(job_id) / "output" / "royalty_checks.csv"
        if not target.exists():
            abort(404)
        return send_file(target, as_attachment=True, download_name=f"royalty_checks_{job_id}.csv")

    @app.get("/preview/<job_id>")
    def preview_csv(job_id: str) -> str:
        target = RUNS_DIR / secure_filename(job_id) / "output" / "royalty_checks.csv"
        if not target.exists():
            abort(404)

        headers: list[str] = []
        rows: list[list[str]] = []
        total_rows = 0
        preview_limit = 200

        with target.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            headers = next(reader, [])
            for row in reader:
                total_rows += 1
                if len(rows) < preview_limit:
                    rows.append(row)

        return render_template(
            "preview.html",
            job_id=secure_filename(job_id),
            csv_name=target.name,
            headers=headers,
            rows=rows,
            total_rows=total_rows,
            shown_rows=len(rows),
            brand_image=find_brand_image(),
        )

    @app.get("/brand-pdf")
    def download_brand_pdf() -> Any:
        relative = find_brand_pdf()
        if not relative:
            abort(404)
        target = BASE_DIR / relative
        if not target.exists():
            abort(404)
        return send_file(target, as_attachment=False, download_name=target.name)

    @app.get("/brand-image")
    def download_brand_image() -> Any:
        relative = find_brand_image()
        if not relative:
            abort(404)
        target = BASE_DIR / relative
        if not target.exists():
            abort(404)
        return send_file(target, as_attachment=False, download_name=target.name)

    return app


def main() -> None:
    app = create_app()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=False)


if __name__ == "__main__":
    main()

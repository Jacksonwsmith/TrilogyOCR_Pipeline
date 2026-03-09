"""Flask web UI for running the Trilogy OCR pipeline."""

from __future__ import annotations

import os
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from flask import Flask, abort, render_template, request, send_file
from werkzeug.utils import secure_filename

from . import pipeline

BASE_DIR = Path(__file__).resolve().parents[2]
RUNS_DIR = BASE_DIR / "web_runs"
PERSISTENT_CHECKS_DIR = BASE_DIR / "checks"
RUNS_DIR.mkdir(parents=True, exist_ok=True)
PERSISTENT_CHECKS_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class JobResult:
    job_id: str
    created_at: str
    input_count: int
    output_csv: Path | None
    rows_written: int
    error: str | None


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
        rows_written = 0
        error: str | None = None

        try:
            rows_written = pipeline.process_checks_to_csv(str(checks_dir), str(output_csv))
        except Exception as exc:  # pragma: no cover - runtime surface
            error = f"{exc}\n\n{traceback.format_exc()}"
            output_csv = None

        result = JobResult(
            job_id=job_id,
            created_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            input_count=len(pdf_files),
            output_csv=output_csv if output_csv and output_csv.exists() else None,
            rows_written=rows_written,
            error=error,
        )
        return render_template("result.html", result=result)

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

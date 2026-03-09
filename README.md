# Trilogy OCR Pipeline

Production-oriented OCR pipeline that converts royalty-check PDFs into a normalized CSV using PyMuPDF and Mistral vision models.

## What this app does
- Reads all `.pdf` files from an input folder.
- Renders each page to an image.
- Calls Mistral vision to extract one JSON object per detail line.
- Writes a fixed-schema CSV as `royalty_checks.csv`.

## Project layout
- `src/trilogy_ocr/pipeline.py`: Core extraction and transform logic.
- `src/trilogy_ocr/cli.py`: Command-line entrypoint.
- `trilogy_ocr_pipeline.py`: Backward-compatible script wrapper.
- `tests/`: Unit tests for parser/normalization/row mapping.

## Quick start
1. Create a virtual environment.
```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install app dependencies.
```bash
pip install -r requirements.txt
```

3. Configure environment.
```bash
cp .env.example .env
```
Set `MISTRAL_API_KEY` in `.env`.

4. Put input files in `checks/` and run pipeline.
```bash
trilogy-ocr --pdf-folder ./checks --output-csv ./royalty_checks.csv
```

## Colab-style workflow parity
This repo now includes the same flow as your pasted code:
- create `checks/`
- move uploaded files into `checks/`
- process all PDFs and write `royalty_checks.csv`

If you're in Colab and using `uploaded = files.upload()`, you can mirror your snippet with:
```python
from trilogy_ocr.pipeline import move_uploaded_files_to_checks, process_checks_to_csv

check_files = move_uploaded_files_to_checks(uploaded, "checks")
print(f"Found {len(check_files)} PDF file(s) in checks folder")
process_checks_to_csv("./checks", "royalty_checks.csv")
```

## Web UI
Run a local browser UI:
```bash
trilogy-ocr-web
```

One-line run (install + launch):
```bash
pip install -r requirements.txt && trilogy-ocr-web
```

Then open `http://localhost:8080` and:
- Enter `MISTRAL_API_KEY` (or keep it in your env/.env).
- Upload one or more PDF files.
- Run extraction and download the generated CSV.

## CLI usage
```bash
trilogy-ocr --help
python3 trilogy_ocr_pipeline.py --help
trilogy-ocr-web
```

Arguments:
- `--pdf-folder`: input folder with PDF files (default `./checks`)
- `--output-csv`: output CSV path (default `./output/royalty_checks.csv`)
- `--debug`: verbose logging

## Environment variables
- `MISTRAL_API_KEY` (required)
- `MISTRAL_MODEL` (default `pixtral-large-latest`)
- `PDF_RENDER_DPI` (default `220`)
- `MISTRAL_MAX_TOKENS` (default `30000`)
- `MISTRAL_MAX_RETRIES` (default `1`)
- `RETRY_DELAY_SECONDS` (default `2`)

## Development
Install dev dependencies:
```bash
pip install -r requirements-dev.txt
```

Run checks:
```bash
ruff check .
pytest
```

## Notes
- Output is CSV. JSON is an intermediate model response format, not a persisted artifact.
- Keep API keys in `.env`; `.env` is ignored by git.

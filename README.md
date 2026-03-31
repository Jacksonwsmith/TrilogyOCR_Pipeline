# Trilogy OCR Pipeline

Extract royalty-check detail lines from PDFs into a normalized CSV using Mistral Vision.

## Start Here
If you just want to run the app:

```bash
./run_web.sh
```

It will:
- create `.venv` if needed
- install requirements
- load `.env`
- start the web app at `http://localhost:8080`
- auto-open the browser on macOS

## What You Get
- Web app for upload/run/download flow
- CLI for batch runs
- CSV output with fixed schema (`royalty_checks.csv`)
- Live run status with progress, elapsed time, ETA, and per-page timings
- Cancel button during extraction
- CSV preview page in the browser

## Project Structure
- `src/trilogy_ocr/pipeline.py`: extraction pipeline + CSV mapping
- `src/trilogy_ocr/web.py`: Flask app + job lifecycle
- `src/trilogy_ocr/templates/`: web pages (`index`, `result`, `preview`)
- `src/trilogy_ocr/static/app.css`: UI styles
- `trilogy_ocr_pipeline.py`: wrapper script entrypoint
- `run_web.sh`: one-command local run
- `checks/`: local input PDFs
- `web_runs/`: per-run artifacts (uploaded PDFs + generated CSV)
- `tests/`: test suite

## Setup (Manual)

1. Create and activate a virtual environment.
```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies.
```bash
pip install -r requirements.txt
```

3. Create `.env` from example.
```bash
cp .env.example .env
```

4. Add your API key in `.env`.
```env
MISTRAL_API_KEY=your_real_key_here
```

## Run Modes

### Web App (recommended)
```bash
./run_web.sh
```

Then open `http://localhost:8080`.

### CLI
```bash
python trilogy_ocr_pipeline.py --pdf-folder ./checks --output-csv ./royalty_checks.csv --debug
```

or

```bash
trilogy-ocr --pdf-folder ./checks --output-csv ./royalty_checks.csv --debug
```

## Environment Variables
- `MISTRAL_API_KEY` required
- `MISTRAL_MODEL` default `pixtral-large-latest`
- `PDF_RENDER_DPI` default `220`
- `MISTRAL_MAX_TOKENS` default `30000`
- `MISTRAL_MAX_RETRIES` default `1`
- `RETRY_DELAY_SECONDS` default `2`
- `PAGE_SEGMENT_FALLBACK_PARTS` default `2`
- `PAGE_SEGMENT_OVERLAP_PX` default `120`
- `SEGMENT_PASS_ALWAYS` default `1`

## Troubleshooting

### 401 Unauthorized
Your key is invalid/missing in environment.

Check quickly:
```bash
set -a && source .env && set +a
python - <<'PY'
from mistralai import Mistral
import os
client = Mistral(api_key=os.environ['MISTRAL_API_KEY'])
print('AUTH_OK model_count:', len(client.models.list().data))
PY
```

### No rows or missing rows
- Ensure PDF has readable detail lines
- Keep `PDF_RENDER_DPI` near `200-300`
- Keep segmentation enabled (`SEGMENT_PASS_ALWAYS=1`)
- Review per-page timings on the run page to spot slow/failing pages

### App doesn’t reflect code changes
Restart app and hard refresh:
- stop with `Ctrl+C`
- rerun `./run_web.sh`
- browser hard refresh `Cmd+Shift+R`

## Development
Install dev dependencies:
```bash
pip install -r requirements-dev.txt
```

Run checks:
```bash
pytest
```

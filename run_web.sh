#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

if [ ! -d ".venv" ]; then
  echo "Creating virtual environment..."
  python3 -m venv .venv
fi

source .venv/bin/activate

echo "Installing requirements..."
python -m pip install --quiet --disable-pip-version-check -r requirements.txt

if [ -f ".env" ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

if [ -z "${MISTRAL_API_KEY:-}" ]; then
  echo "MISTRAL_API_KEY is not set. Put it in .env or export it before running."
  exit 1
fi

echo "Starting web app on http://localhost:8080"

if command -v open >/dev/null 2>&1; then
  (sleep 1 && open "http://localhost:8080") >/dev/null 2>&1 &
fi

exec trilogy-ocr-web

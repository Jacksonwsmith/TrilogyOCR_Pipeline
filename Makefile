.PHONY: install dev lint test run

install:
	pip install -r requirements.txt

dev:
	pip install -r requirements-dev.txt

lint:
	ruff check .

test:
	pytest

run:
	trilogy-ocr --pdf-folder ./checks --output-csv ./output/royalty_checks.csv

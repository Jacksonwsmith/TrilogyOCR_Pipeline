"""CLI for Trilogy OCR pipeline."""

from __future__ import annotations

import argparse
import logging

from .pipeline import process_checks_to_csv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Trilogy OCR pipeline on royalty check PDFs.")
    parser.add_argument("--pdf-folder", default="./checks", help="Folder containing input PDF files.")
    parser.add_argument("--output-csv", default="./royalty_checks.csv", help="Output CSV path.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, format="%(levelname)s | %(message)s")
    process_checks_to_csv(pdf_folder=args.pdf_folder, output_csv=args.output_csv)


if __name__ == "__main__":
    main()

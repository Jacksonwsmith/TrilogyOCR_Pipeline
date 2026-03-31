#!/usr/bin/env python3
"""Backward-compatible script entrypoint."""

import sys
from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parent
    src = root / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))

    from trilogy_ocr.cli import main as cli_main
    cli_main()

if __name__ == "__main__":
    main()

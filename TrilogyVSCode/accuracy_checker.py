#!/usr/bin/env python3
"""
Simple CSV accuracy checker.

Compares an "expected" CSV to an "actual" CSV and reports mismatches.
Usage: python accuracy_checker.py expected.csv actual.csv
"""
from __future__ import annotations

import argparse
import csv
import math
from typing import List, Tuple


def read_csv(path: str) -> List[List[str]]:
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        return [row for row in reader]


def is_float(s: str) -> bool:
    try:
        float(s)
        return True
    except Exception:
        return False


def compare_csv(expected: List[List[str]], actual: List[List[str]], tol: float = 1e-9) -> Tuple[int, List[Tuple[int,int,str,str,float,bool]]]:
    """
    Compare two CSV contents. Returns (mismatch_count, list_of_mismatches)

    Each mismatch is (row_index, col_index, expected_value, actual_value, numeric_diff, match_boolean)
    row_index and col_index are 0-based and include header row if present.
    """
    mismatches = []

    # If shapes differ, we'll compare up to the min dims and record missing cells
    max_rows = max(len(expected), len(actual))
    max_cols = max((len(r) for r in expected), default=0, )
    max_cols = max(max_cols, max((len(r) for r in actual), default=0))

    for r in range(max_rows):
        erow = expected[r] if r < len(expected) else []
        arow = actual[r] if r < len(actual) else []
        for c in range(max_cols):
            e = erow[c] if c < len(erow) else ''
            a = arow[c] if c < len(arow) else ''

            if e == a:
                continue

            # Try numeric comparison when both look like floats
            if is_float(e) and is_float(a):
                en = float(e)
                an = float(a)
                diff = en - an
                if math.isfinite(diff) and abs(diff) <= tol:
                    continue
                mismatches.append((r, c, e, a, diff, False))
            else:
                mismatches.append((r, c, e, a, float('nan'), False))

    return len(mismatches), mismatches


def write_report(path: str, mismatches: List[Tuple[int,int,str,str,float,bool]]) -> None:
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['row', 'col', 'expected', 'actual', 'numeric_diff'])
        for r, c, e, a, diff, _ in mismatches:
            writer.writerow([r, c, e, a, '' if math.isnan(diff) else diff])


def main():
    p = argparse.ArgumentParser(description='Compare expected CSV to actual CSV and report mismatches.')
    p.add_argument('expected')
    p.add_argument('actual')
    p.add_argument('--tol', type=float, default=1e-9, help='numeric tolerance for float comparisons')
    p.add_argument('--report', default='accuracy_report.csv', help='path to write mismatch report')
    args = p.parse_args()

    expected = read_csv(args.expected)
    actual = read_csv(args.actual)

    mismatches_count, mismatches = compare_csv(expected, actual, tol=args.tol)

    if mismatches_count == 0:
        print('MATCH: files are identical within tolerance')
        return 0

    print(f'MISMATCHES: {mismatches_count} differences found')
    write_report(args.report, mismatches)
    print(f'Wrote mismatch report to: {args.report}')
    return 2


if __name__ == '__main__':
    raise SystemExit(main())

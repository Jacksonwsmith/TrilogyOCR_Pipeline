# CSV Accuracy Checker

Simple utility to compare an expected CSV to an actual CSV and produce a mismatch report.

Usage:

```bash
python accuracy_checker.py expected.csv actual.csv --tol 1e-6 --report my_report.csv
python WJF-Diversified_Check_V1.py my_statement.pdf -o output.csv
python WFJ-Diversified_Check_V2 my_statement.pdf -o output.csv
```

Output:
- prints whether files match
- writes a CSV report with rows: `row`, `col`, `expected`, `actual`, `numeric_diff`

Notes:
- Numeric values are compared using the provided tolerance (`--tol`).
- The checker compares by row/column position. If your outputs are in different row order, sort them first or adapt the script.

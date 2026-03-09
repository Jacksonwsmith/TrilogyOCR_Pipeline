from trilogy_ocr.pipeline import (
    _normalize_date_mdy,
    dedupe_adjacent_rows,
    json_row_to_csv_row,
    parse_json_array_loose,
)


def test_parse_json_array_loose_strips_fences():
    raw = """```json
    [{\"Operator_ID\":\"X\"}]
    ```"""
    rows = parse_json_array_loose(raw)
    assert rows == [{"Operator_ID": "X"}]


def test_normalize_date_formats():
    assert _normalize_date_mdy("2026-03-09") == "3/9/2026"
    assert _normalize_date_mdy("03/09/26") == "3/9/2026"


def test_json_row_to_csv_row_maps_arrays_and_cleans_numeric():
    row = json_row_to_csv_row(
        {
            "Check_Amount": "1,593.71",
            "Check_Date": "2026-03-09",
            "Taxes": [{"Tax_Code": "TX", "Tax_Type": "STATE", "Gross_Tax": "1,000.00", "Net_Tax": "900.00"}],
            "Deductions": [
                {
                    "Deduct_Code": "D1",
                    "Deduct_Type": "FEE",
                    "Gross_Deduct": "50,000.00",
                    "Net_Deduct": "49,500.00",
                }
            ],
        }
    )

    assert row["Check Amount"] == "1593.71"
    assert row["Check Date"] == "3/9/2026"
    assert row["Tax Code 1"] == "TX"
    assert row["Gross Tax 1"] == "1000.00"
    assert row["Deduct Code 1"] == "D1"
    assert row["Gross Deduct 1"] == "50000.00"


def test_dedupe_adjacent_rows_only_removes_consecutive_duplicates():
    rows = [{"a": "1"}, {"a": "1"}, {"a": "2"}, {"a": "1"}]
    out = dedupe_adjacent_rows(rows)
    assert out == [{"a": "1"}, {"a": "2"}, {"a": "1"}]

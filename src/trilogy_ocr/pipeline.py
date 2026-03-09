"""Core OCR pipeline for royalty-check PDF extraction."""

from __future__ import annotations

import base64
import csv
import io
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None  # type: ignore

try:
    from mistralai import Mistral
except Exception:  # pragma: no cover
    Mistral = Any  # type: ignore

try:
    from PIL import Image
except Exception:  # pragma: no cover
    Image = Any  # type: ignore

MISTRAL_API_KEY = os.environ.get("MISTRAL_API_KEY", "").strip()

MODEL = os.getenv("MISTRAL_MODEL", "pixtral-large-latest")
DPI = int(os.getenv("PDF_RENDER_DPI", "220"))
MAX_TOKENS = int(os.getenv("MISTRAL_MAX_TOKENS", "30000"))
MAX_RETRIES = int(os.getenv("MISTRAL_MAX_RETRIES", "1"))
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "2"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

PROMPT = """You are extracting data from an oil & gas royalty check statement. Extract EVERY production detail line.

For EACH line in the revenue statement table, create a separate JSON object in an array.

Each object should have these EXACT field names (use empty string "" for missing values):
{
  "Operator_ID": "",
  "Operator_Name": "DIVERSIFIED",
  "Owner_Name": "Wilson Johnson Family",
  "Owner_Number": "1064941",
  "Check_Number": "5001502951",
  "Check_Date": "1/27/2026",
  "Check_Amount": "1031.33",
  "Operator_CC": "1119847.01",
  "Operator_API": "4703305324",
  "Partner_API": "",
  "MA_API": "",
  "Partner_CC": "",
  "Property_Description": "FORTNEY D 156, TOWN/DISTRICT: 04 EAGLE",
  "Property_State": "WV",
  "Property_County": "HARRISON",
  "Product_Code": "",
  "Product_Description": "Gas",
  "Interest_Code": "",
  "Interest_Type": "Royalty Interest",
  "Owner_Percent": "0.0625",
  "Distribution_Percent": "0.0625",
  "Prod_Date": "11/1/2025",
  "Price": "2.63",
  "BTU_Factor": "1.0002",
  "Gross_Volume": "605.27",
  "Gross_Value": "1593.71",
  "Gross_Taxes": "0",
  "Gross_Deducts": "0",
  "Net_Value": "1593.71",
  "Owner_Gross_Volume": "37.83",
  "Owner_Gross_Value": "99.61",
  "Owner_Gross_Taxes": "0",
  "Owner_Gross_Deducts": "0",
  "Owner_Net_Value": "99.61",
  "Tax_Code_1": "", "Tax_Type_1": "", "Gross_Tax_1": "", "Net_Tax_1": "",
  "Tax_Code_2": "", "Tax_Type_2": "", "Gross_Tax_2": "", "Net_Tax_2": "",
  "Tax_Code_3": "", "Tax_Type_3": "", "Gross_Tax_3": "", "Net_Tax_3": "",
  "Tax_Code_4": "", "Tax_Type_4": "", "Gross_Tax_4": "", "Net_Tax_4": "",
  "Tax_Code_5": "", "Tax_Type_5": "", "Gross_Tax_5": "", "Net_Tax_5": "",
  "Tax_Code_6": "", "Tax_Type_6": "", "Gross_Tax_6": "", "Net_Tax_6": "",
  "Tax_Code_7": "", "Tax_Type_7": "", "Gross_Tax_7": "", "Net_Tax_7": "",
  "Tax_Code_8": "", "Tax_Type_8": "", "Gross_Tax_8": "", "Net_Tax_8": "",
  "Tax_Code_9": "", "Tax_Type_9": "", "Gross_Tax_9": "", "Net_Tax_9": "",
  "Tax_Code_10": "", "Tax_Type_10": "", "Gross_Tax_10": "", "Net_Tax_10": "",
  "Deduct_Code_1": "", "Deduct_Type_1": "", "Gross_Deduct_1": "", "Net_Deduct_1": "",
  "Deduct_Code_2": "", "Deduct_Type_2": "", "Gross_Deduct_2": "", "Net_Deduct_2": "",
  "Deduct_Code_3": "", "Deduct_Type_3": "", "Gross_Deduct_3": "", "Net_Deduct_3": "",
  "Deduct_Code_4": "", "Deduct_Type_4": "", "Gross_Deduct_4": "", "Net_Deduct_4": "",
  "Deduct_Code_5": "", "Deduct_Type_5": "", "Gross_Deduct_5": "", "Net_Deduct_5": "",
  "Deduct_Code_6": "", "Deduct_Type_6": "", "Gross_Deduct_6": "", "Net_Deduct_6": "",
  "Deduct_Code_7": "", "Deduct_Type_7": "", "Gross_Deduct_7": "", "Net_Deduct_7": "",
  "Deduct_Code_8": "", "Deduct_Type_8": "", "Gross_Deduct_8": "", "Net_Deduct_8": "",
  "Deduct_Code_9": "", "Deduct_Type_9": "", "Gross_Deduct_9": "", "Net_Deduct_9": "",
  "Deduct_Code_10": "", "Deduct_Type_10": "", "Gross_Deduct_10": "", "Net_Deduct_10": "",
  "Detail_Line_Notation": ""
}

CRITICAL INSTRUCTIONS:
- Operator_CC = the Property number (e.g., "1119847.01")
- Operator_API = the "Operator API#" number (e.g., "4703305324")
- Product_Description = "Gas" or "Oil" from the Type column
- Interest_Type = "Royalty Interest" or "FRR1" from the Type column
- Create ONE object per line in the revenue table
- Return a JSON ARRAY with ALL detail lines found on this page
- Use empty strings "" for any missing data
- Remove commas from numbers (e.g., "1,593.71" becomes "1593.71")
- Format dates as M/D/YYYY (e.g., "11/1/2025")

Return ONLY the JSON array, no markdown, no explanation.
"""

CSV_COLUMNS = [
    "Operator ID",
    "Operator Name",
    "Owner Name",
    "Owner Number",
    "Check Number",
    "Check Date",
    "Check Amount",
    "Operator CC",
    "Operator API",
    "Partner API",
    "MA API",
    "Partner CC",
    "Property Description",
    "Property State",
    "Property County",
    "Product Code",
    "Product Description",
    "Interest Code",
    "Interest Type",
    "Owner Percent",
    "Distribution Percent",
    "Prod Date",
    "Price",
    "BTU Factor",
    "Gross Volume",
    "Gross Value",
    "Gross Taxes",
    "Gross Deducts",
    "Net Value",
    "Owner Gross Volume",
    "Owner Gross Value",
    "Owner Gross Taxes",
    "Owner Gross Deducts",
    "Owner Net Value",
]

for i in range(1, 11):
    CSV_COLUMNS += [f"Tax Code {i}", f"Tax Type {i}", f"Gross Tax {i}", f"Net Tax {i}"]
for i in range(1, 11):
    CSV_COLUMNS += [f"Deduct Code {i}", f"Deduct Type {i}", f"Gross Deduct {i}", f"Net Deduct {i}"]

CSV_COLUMNS += ["Detail Line Notation"]

JSON_TO_CSV = {
    "Operator_ID": "Operator ID",
    "Operator_Name": "Operator Name",
    "Owner_Name": "Owner Name",
    "Owner_Number": "Owner Number",
    "Check_Number": "Check Number",
    "Check_Date": "Check Date",
    "Check_Amount": "Check Amount",
    "Operator_CC": "Operator CC",
    "Operator_API": "Operator API",
    "Partner_API": "Partner API",
    "MA_API": "MA API",
    "Partner_CC": "Partner CC",
    "Property_Description": "Property Description",
    "Property_State": "Property State",
    "Property_County": "Property County",
    "Product_Code": "Product Code",
    "Product_Description": "Product Description",
    "Interest_Code": "Interest Code",
    "Interest_Type": "Interest Type",
    "Owner_Percent": "Owner Percent",
    "Distribution_Percent": "Distribution Percent",
    "Prod_Date": "Prod Date",
    "Price": "Price",
    "BTU_Factor": "BTU Factor",
    "Gross_Volume": "Gross Volume",
    "Gross_Value": "Gross Value",
    "Gross_Taxes": "Gross Taxes",
    "Gross_Deducts": "Gross Deducts",
    "Net_Value": "Net Value",
    "Owner_Gross_Volume": "Owner Gross Volume",
    "Owner_Gross_Value": "Owner Gross Value",
    "Owner_Gross_Taxes": "Owner Gross Taxes",
    "Owner_Gross_Deducts": "Owner Gross Deducts",
    "Owner_Net_Value": "Owner Net Value",
    "Detail_Line_Notation": "Detail Line Notation",
}


def require_api_key() -> str:
    key = MISTRAL_API_KEY.strip()
    if not key:
        raise RuntimeError(
            "Missing MISTRAL_API_KEY. Set environment variable or set trilogy_ocr.pipeline.MISTRAL_API_KEY."
        )
    return key


def pdf_page_to_base64_jpeg(doc: Any, page_num: int, dpi: int = 220) -> Tuple[str, str]:
    if Image is Any:
        raise RuntimeError("Missing pillow dependency. Install with `pip install -r requirements.txt`.")
    page = doc[page_num]
    pix = page.get_pixmap(dpi=max(72, min(dpi, 350)))
    mode = "RGB"
    img = Image.frombytes("RGB" if pix.alpha == 0 else "RGBA", [pix.width, pix.height], pix.samples)
    if img.mode != mode:
        img = img.convert(mode)

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=92, optimize=True)
    return base64.b64encode(buf.getvalue()).decode("utf-8"), "image/jpeg"


def strip_code_fences(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        lines = t.splitlines()
        lines = lines[1:-1]
        t = "\n".join(lines).strip()
        if t.lower().startswith("json"):
            t = t[4:].strip()
    return t


def parse_json_array_loose(text: str) -> List[Dict[str, Any]]:
    raw = strip_code_fences(text)
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return [parsed]
        if isinstance(parsed, list):
            return [x for x in parsed if isinstance(x, dict)]
        return []
    except Exception:
        pass

    start = raw.find("[")
    end = raw.rfind("]")
    if start != -1 and end != -1 and end > start:
        candidate = raw[start : end + 1]
        parsed = json.loads(candidate)
        if isinstance(parsed, list):
            return [x for x in parsed if isinstance(x, dict)]

    raise ValueError("Could not parse JSON array from model output.")


def _normalize_date_mdy(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            dt = datetime.strptime(text, fmt)
            return f"{dt.month}/{dt.day}/{dt.year}"
        except ValueError:
            continue
    return text


def _strip_number_commas(value: str) -> str:
    return (value or "").replace(",", "")


def json_row_to_csv_row(item: Dict[str, Any]) -> Dict[str, str]:
    row = {col: "" for col in CSV_COLUMNS}
    for jkey, ckey in JSON_TO_CSV.items():
        row[ckey] = str(item.get(jkey, "") if item.get(jkey, "") is not None else "")

    for i in range(1, 11):
        row[f"Tax Code {i}"] = str(item.get(f"Tax_Code_{i}", "") if item.get(f"Tax_Code_{i}", "") is not None else "")
        row[f"Tax Type {i}"] = str(item.get(f"Tax_Type_{i}", "") if item.get(f"Tax_Type_{i}", "") is not None else "")
        row[f"Gross Tax {i}"] = str(item.get(f"Gross_Tax_{i}", "") if item.get(f"Gross_Tax_{i}", "") is not None else "")
        row[f"Net Tax {i}"] = str(item.get(f"Net_Tax_{i}", "") if item.get(f"Net_Tax_{i}", "") is not None else "")

    for i in range(1, 11):
        row[f"Deduct Code {i}"] = str(item.get(f"Deduct_Code_{i}", "") if item.get(f"Deduct_Code_{i}", "") is not None else "")
        row[f"Deduct Type {i}"] = str(item.get(f"Deduct_Type_{i}", "") if item.get(f"Deduct_Type_{i}", "") is not None else "")
        row[f"Gross Deduct {i}"] = str(item.get(f"Gross_Deduct_{i}", "") if item.get(f"Gross_Deduct_{i}", "") is not None else "")
        row[f"Net Deduct {i}"] = str(item.get(f"Net_Deduct_{i}", "") if item.get(f"Net_Deduct_{i}", "") is not None else "")

    taxes = item.get("Taxes", [])
    if isinstance(taxes, list):
        for idx, tax in enumerate(taxes[:10], start=1):
            if not isinstance(tax, dict):
                continue
            if not row[f"Tax Code {idx}"]:
                row[f"Tax Code {idx}"] = str(tax.get("Tax_Code", "") or "")
            if not row[f"Tax Type {idx}"]:
                row[f"Tax Type {idx}"] = str(tax.get("Tax_Type", "") or "")
            if not row[f"Gross Tax {idx}"]:
                row[f"Gross Tax {idx}"] = str(tax.get("Gross_Tax", "") or "")
            if not row[f"Net Tax {idx}"]:
                row[f"Net Tax {idx}"] = str(tax.get("Net_Tax", "") or "")

    deductions = item.get("Deductions", [])
    if isinstance(deductions, list):
        for idx, deduct in enumerate(deductions[:10], start=1):
            if not isinstance(deduct, dict):
                continue
            if not row[f"Deduct Code {idx}"]:
                row[f"Deduct Code {idx}"] = str(deduct.get("Deduct_Code", "") or "")
            if not row[f"Deduct Type {idx}"]:
                row[f"Deduct Type {idx}"] = str(deduct.get("Deduct_Type", "") or "")
            if not row[f"Gross Deduct {idx}"]:
                row[f"Gross Deduct {idx}"] = str(deduct.get("Gross_Deduct", "") or "")
            if not row[f"Net Deduct {idx}"]:
                row[f"Net Deduct {idx}"] = str(deduct.get("Net_Deduct", "") or "")

    for col in ("Check Amount", "Price", "BTU Factor", "Gross Volume", "Gross Value", "Net Value"):
        row[col] = _strip_number_commas(row[col])
    row["Check Date"] = _normalize_date_mdy(row["Check Date"])
    row["Prod Date"] = _normalize_date_mdy(row["Prod Date"])
    for i in range(1, 11):
        row[f"Gross Tax {i}"] = _strip_number_commas(row[f"Gross Tax {i}"])
        row[f"Net Tax {i}"] = _strip_number_commas(row[f"Net Tax {i}"])
        row[f"Gross Deduct {i}"] = _strip_number_commas(row[f"Gross Deduct {i}"])
        row[f"Net Deduct {i}"] = _strip_number_commas(row[f"Net Deduct {i}"])

    return row


def dedupe_adjacent_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    if not rows:
        return []
    out = [rows[0]]
    for row in rows[1:]:
        if row != out[-1]:
            out.append(row)
    return out


def ask_model_for_page(client: Mistral, image_b64: str, media_type: str) -> List[Dict[str, Any]]:
    last_error: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.chat.complete(
                model=MODEL,
                temperature=0,
                max_tokens=MAX_TOKENS,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "image_url", "image_url": f"data:{media_type};base64,{image_b64}"},
                            {"type": "text", "text": PROMPT},
                        ],
                    }
                ],
            )
            content = response.choices[0].message.content
            if not isinstance(content, str):
                raise ValueError("Model response content was not a string.")
            return parse_json_array_loose(content)
        except Exception as exc:
            last_error = exc
            logging.warning(f"Attempt {attempt} failed: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS * attempt)
    raise RuntimeError(f"Model call failed after {MAX_RETRIES} attempts: {last_error}")


def process_checks_to_csv(pdf_folder: str, output_csv: str) -> int:
    if fitz is None:
        raise RuntimeError("Missing pymupdf dependency. Install with `pip install -r requirements.txt`.")
    if Mistral is Any:
        raise RuntimeError("Missing mistralai dependency. Install with `pip install -r requirements.txt`.")

    pdf_dir = Path(pdf_folder)
    if not pdf_dir.exists() or not pdf_dir.is_dir():
        raise FileNotFoundError(f"PDF folder does not exist: {pdf_folder}")

    pdf_paths = sorted([p for p in pdf_dir.iterdir() if p.suffix.lower() == ".pdf"])
    if not pdf_paths:
        print("No PDF files found.")
        return 0

    api_key = require_api_key()
    client = Mistral(api_key=api_key)
    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    print(f"Found {len(pdf_paths)} PDF file(s). Processing...")

    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_COLUMNS)
        writer.writeheader()

        for pdf_path in pdf_paths:
            print(f"Processing {pdf_path.name}")
            file_rows = 0
            try:
                doc = fitz.open(pdf_path)
                for page_num in range(doc.page_count):
                    image_b64, media_type = pdf_page_to_base64_jpeg(doc, page_num, dpi=DPI)
                    detail_lines = ask_model_for_page(client, image_b64, media_type)
                    for line in detail_lines:
                        writer.writerow(json_row_to_csv_row(line))
                        file_rows += 1
                        total_rows += 1
                    print(f"  Page {page_num + 1}: {len(detail_lines)} detail line(s)")
                doc.close()
                print(f"  Done {pdf_path.name}: {file_rows} rows")
            except Exception as exc:
                logging.error(f"  ERROR in {pdf_path.name}: {exc}", exc_info=True)

    print(f"Done. Wrote {total_rows} row(s) to {output_path}")
    return total_rows


def move_uploaded_files_to_checks(uploaded: Dict[str, Any], checks_dir: str = "checks") -> List[str]:
    os.makedirs(checks_dir, exist_ok=True)
    for filename in uploaded.keys():
        os.rename(filename, os.path.join(checks_dir, filename))

    return sorted([f for f in os.listdir(checks_dir) if f.lower().endswith(".pdf")])


if __name__ == "__main__":
    process_checks_to_csv(pdf_folder="./checks", output_csv="royalty_checks.csv")

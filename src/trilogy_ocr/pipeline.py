"""Core OCR pipeline for royalty-check PDF extraction."""

from __future__ import annotations

import base64
import csv
import io
import json
import logging
import os
import random
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Sequence, Tuple

try:
    import fitz  # PyMuPDF
except ImportError:  # pragma: no cover
    fitz = None  # type: ignore

try:
    from mistralai import Mistral
except ImportError:  # pragma: no cover
    Mistral = Any  # type: ignore

try:
    from PIL import Image
except ImportError:  # pragma: no cover
    Image = Any  # type: ignore

if TYPE_CHECKING:
    import fitz as fitz_type


def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return
    load_dotenv()


_load_dotenv_if_available()

MODEL = os.getenv("MISTRAL_MODEL", "pixtral-large-latest")
DPI = int(os.getenv("PDF_RENDER_DPI", "220"))
MAX_TOKENS = int(os.getenv("MISTRAL_MAX_TOKENS", "30000"))
MAX_RETRIES = int(os.getenv("MISTRAL_MAX_RETRIES", "4"))
RETRY_BASE_DELAY_SECONDS = float(os.getenv("MISTRAL_RETRY_BASE_DELAY_SECONDS", "1.5"))
RETRY_JITTER_SECONDS = float(os.getenv("MISTRAL_RETRY_JITTER_SECONDS", "0.4"))
PAGE_SEGMENT_FALLBACK_PARTS = int(os.getenv("PAGE_SEGMENT_FALLBACK_PARTS", "2"))
PAGE_SEGMENT_OVERLAP_PX = int(os.getenv("PAGE_SEGMENT_OVERLAP_PX", "80"))
MAX_IMAGE_EDGE = int(os.getenv("MAX_IMAGE_EDGE", "2600"))
JPEG_QUALITY = int(os.getenv("JPEG_QUALITY", "88"))
REQUEST_PAUSE_SECONDS = float(os.getenv("REQUEST_PAUSE_SECONDS", "0.0"))

MISTRAL_API_KEY = os.environ.get("MISTRAL_API_KEY", "").strip()

logger = logging.getLogger(__name__)

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


class TruncatedOutputError(RuntimeError):
    """Raised when the model output appears truncated."""


class ParseModelOutputError(ValueError):
    """Raised when model output is not parseable as JSON."""


def _require_runtime_deps() -> None:
    missing: List[str] = []
    if fitz is None:
        missing.append("pymupdf")
    if Image is Any:
        missing.append("pillow")
    if Mistral is Any:
        missing.append("mistralai")
    if missing:
        raise RuntimeError(
            "Missing runtime dependencies: "
            + ", ".join(missing)
            + ". Install with `pip install -r requirements.txt`."
        )


def _build_prompt(segment_hint: str = "") -> str:
    hint = f"\nSegment note: {segment_hint}\n" if segment_hint else "\n"
    return f"""You are extracting data from an oil & gas royalty check statement image.

Extract EVERY production detail line from the revenue statement table.
Create ONE JSON object per detail line.
Return ONLY a JSON array and nothing else.

Each output object must include these keys as strings:
{{
  "Operator_ID": "",
  "Operator_Name": "",
  "Owner_Name": "",
  "Owner_Number": "",
  "Check_Number": "",
  "Check_Date": "",
  "Check_Amount": "",
  "Operator_CC": "",
  "Operator_API": "",
  "Partner_API": "",
  "MA_API": "",
  "Partner_CC": "",
  "Property_Description": "",
  "Property_State": "",
  "Property_County": "",
  "Product_Code": "",
  "Product_Description": "",
  "Interest_Code": "",
  "Interest_Type": "",
  "Owner_Percent": "",
  "Distribution_Percent": "",
  "Prod_Date": "",
  "Price": "",
  "BTU_Factor": "",
  "Gross_Volume": "",
  "Gross_Value": "",
  "Gross_Taxes": "",
  "Gross_Deducts": "",
  "Net_Value": "",
  "Owner_Gross_Volume": "",
  "Owner_Gross_Value": "",
  "Owner_Gross_Taxes": "",
  "Owner_Gross_Deducts": "",
  "Owner_Net_Value": "",
  "Taxes": [
    {{"Tax_Code": "", "Tax_Type": "", "Gross_Tax": "", "Net_Tax": ""}}
  ],
  "Deductions": [
    {{"Deduct_Code": "", "Deduct_Type": "", "Gross_Deduct": "", "Net_Deduct": ""}}
  ],
  "Detail_Line_Notation": ""
}}

Rules:
- Never invent values; only extract what is visible.
- Use empty string "" for missing scalar values.
- "Taxes" and "Deductions" should include only non-empty rows; use [] when none.
- Remove commas from numeric values (e.g., 1,593.71 -> 1593.71).
- Format dates as M/D/YYYY when possible.
- If no detail lines are visible, return [].
{hint}
Return only the JSON array, with no markdown or explanation.
"""


PROMPT = _build_prompt()


def require_api_key() -> str:
    if not MISTRAL_API_KEY:
        raise RuntimeError("Missing MISTRAL_API_KEY. Set it in your environment or .env file.")
    return MISTRAL_API_KEY


def _resampling_filter() -> int:
    try:
        return Image.Resampling.LANCZOS
    except AttributeError:
        return Image.LANCZOS


def render_page_image(doc: "fitz_type.Document", page_num: int, dpi: int) -> Image.Image:
    page = doc[page_num]
    clamped_dpi = max(120, min(dpi, 350))
    pix = page.get_pixmap(dpi=clamped_dpi, alpha=False)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    longest = max(img.width, img.height)
    if longest > MAX_IMAGE_EDGE:
        scale = MAX_IMAGE_EDGE / float(longest)
        new_size = (max(1, int(img.width * scale)), max(1, int(img.height * scale)))
        img = img.resize(new_size, _resampling_filter())
    return img


def split_image_vertical(
    image: Image.Image, parts: int, overlap_px: int
) -> List[Tuple[Image.Image, Tuple[int, int]]]:
    if parts <= 1:
        return [(image, (0, image.height))]

    parts = min(max(2, parts), 6)
    overlap_px = max(0, overlap_px)
    h = image.height
    w = image.width
    step = h / float(parts)
    segments: List[Tuple[Image.Image, Tuple[int, int]]] = []

    for i in range(parts):
        top = int(i * step)
        bottom = int((i + 1) * step)
        if i > 0:
            top = max(0, top - overlap_px)
        if i < parts - 1:
            bottom = min(h, bottom + overlap_px)
        crop = image.crop((0, top, w, bottom))
        segments.append((crop, (top, bottom)))
    return segments


def image_to_base64_jpeg(img: Image.Image) -> Tuple[str, str]:
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=max(45, min(JPEG_QUALITY, 95)), optimize=True)
    return base64.b64encode(buf.getvalue()).decode("utf-8"), "image/jpeg"


def strip_code_fences(text: str) -> str:
    t = text.strip()
    if not t.startswith("```"):
        return t
    lines = t.splitlines()
    if len(lines) >= 2:
        lines = lines[1:-1]
    t = "\n".join(lines).strip()
    if t.lower().startswith("json"):
        t = t[4:].strip()
    return t


def likely_truncated(raw_text: str) -> bool:
    s = raw_text.strip()
    if not s:
        return True
    if s.startswith("[") and not s.endswith("]"):
        return True
    if s.count("[") > s.count("]"):
        return True
    return False


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
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError as exc:
            raise ParseModelOutputError("Could not parse JSON array from model output.") from exc
        if isinstance(parsed, list):
            return [x for x in parsed if isinstance(x, dict)]
        if isinstance(parsed, dict):
            return [parsed]

    raise ParseModelOutputError("Could not parse JSON array from model output.")


def _get_attr(obj: Any, name: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _extract_message_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        chunks: List[str] = []
        for item in content:
            text = _get_attr(item, "text", "")
            if not text and isinstance(item, dict):
                text = str(item.get("content", ""))
            if text:
                chunks.append(str(text))
        return "\n".join(chunks).strip()
    return str(content)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


_NUMERIC_COLUMNS = {
    "Owner Percent",
    "Distribution Percent",
    "Check Amount",
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
}


def _clean_numeric_text(value: str) -> str:
    v = value.strip()
    if "," in v:
        v = v.replace(",", "")
    return v


def _normalize_date_mdy(value: str) -> str:
    from datetime import datetime

    v = value.strip()
    if not v:
        return ""
    formats = (
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y-%m-%d",
        "%m-%d-%Y",
        "%m-%d-%y",
    )
    for fmt in formats:
        try:
            dt = datetime.strptime(v, fmt)
            return f"{dt.month}/{dt.day}/{dt.year}"
        except ValueError:
            continue
    return v


def _row_signature(item: Dict[str, Any]) -> str:
    normalized = {k: _normalize_text(item.get(k, "")) for k in sorted(item.keys())}
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


def dedupe_adjacent_rows(items: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    last_sig: str | None = None
    for item in items:
        sig = _row_signature(item)
        if sig == last_sig:
            continue
        out.append(item)
        last_sig = sig
    return out


def _extract_tax_or_deduct_rows(item: Dict[str, Any], key: str, prefix: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    val = item.get(key, [])
    if isinstance(val, list):
        for row in val:
            if not isinstance(row, dict):
                continue
            out.append(
                {
                    f"{prefix}_Code": _normalize_text(row.get(f"{prefix}_Code", "")),
                    f"{prefix}_Type": _normalize_text(row.get(f"{prefix}_Type", "")),
                    f"Gross_{prefix}": _normalize_text(row.get(f"Gross_{prefix}", "")),
                    f"Net_{prefix}": _normalize_text(row.get(f"Net_{prefix}", "")),
                }
            )
    return out


def json_row_to_csv_row(item: Dict[str, Any]) -> Dict[str, str]:
    row = {col: "" for col in CSV_COLUMNS}

    for jkey, ckey in JSON_TO_CSV.items():
        row[ckey] = _normalize_text(item.get(jkey, ""))

    tax_rows = _extract_tax_or_deduct_rows(item, "Taxes", "Tax")
    if not tax_rows:
        for i in range(1, 11):
            tax_rows.append(
                {
                    "Tax_Code": _normalize_text(item.get(f"Tax_Code_{i}", "")),
                    "Tax_Type": _normalize_text(item.get(f"Tax_Type_{i}", "")),
                    "Gross_Tax": _normalize_text(item.get(f"Gross_Tax_{i}", "")),
                    "Net_Tax": _normalize_text(item.get(f"Net_Tax_{i}", "")),
                }
            )

    deduct_rows = _extract_tax_or_deduct_rows(item, "Deductions", "Deduct")
    if not deduct_rows:
        for i in range(1, 11):
            deduct_rows.append(
                {
                    "Deduct_Code": _normalize_text(item.get(f"Deduct_Code_{i}", "")),
                    "Deduct_Type": _normalize_text(item.get(f"Deduct_Type_{i}", "")),
                    "Gross_Deduct": _normalize_text(item.get(f"Gross_Deduct_{i}", "")),
                    "Net_Deduct": _normalize_text(item.get(f"Net_Deduct_{i}", "")),
                }
            )

    for i in range(1, 11):
        idx = i - 1
        if idx < len(tax_rows):
            row[f"Tax Code {i}"] = tax_rows[idx].get("Tax_Code", "")
            row[f"Tax Type {i}"] = tax_rows[idx].get("Tax_Type", "")
            row[f"Gross Tax {i}"] = tax_rows[idx].get("Gross_Tax", "")
            row[f"Net Tax {i}"] = tax_rows[idx].get("Net_Tax", "")
        if idx < len(deduct_rows):
            row[f"Deduct Code {i}"] = deduct_rows[idx].get("Deduct_Code", "")
            row[f"Deduct Type {i}"] = deduct_rows[idx].get("Deduct_Type", "")
            row[f"Gross Deduct {i}"] = deduct_rows[idx].get("Gross_Deduct", "")
            row[f"Net Deduct {i}"] = deduct_rows[idx].get("Net_Deduct", "")

    for col_name, value in list(row.items()):
        if col_name in ("Check Date", "Prod Date"):
            row[col_name] = _normalize_date_mdy(value)
            continue
        if col_name in _NUMERIC_COLUMNS:
            row[col_name] = _clean_numeric_text(value)
            continue
        if (
            col_name.startswith("Gross Tax ")
            or col_name.startswith("Net Tax ")
            or col_name.startswith("Gross Deduct ")
            or col_name.startswith("Net Deduct ")
        ):
            row[col_name] = _clean_numeric_text(value)

    return row


def _sleep_backoff(attempt: int) -> None:
    delay = RETRY_BASE_DELAY_SECONDS * (2 ** max(0, attempt - 1))
    delay += random.uniform(0.0, max(0.0, RETRY_JITTER_SECONDS))
    time.sleep(delay)


def ask_model_for_image(
    client: Mistral, image_b64: str, media_type: str, prompt: str
) -> List[Dict[str, Any]]:
    last_error: Exception | None = None

    for attempt in range(1, max(1, MAX_RETRIES) + 1):
        try:
            response = client.chat.complete(
                model=MODEL,
                temperature=0,
                max_tokens=max(1024, MAX_TOKENS),
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "image_url", "image_url": f"data:{media_type};base64,{image_b64}"},
                            {"type": "text", "text": prompt},
                        ],
                    }
                ],
            )

            first_choice = None
            choices = _get_attr(response, "choices", None)
            if isinstance(choices, Sequence) and choices:
                first_choice = choices[0]

            finish_reason = str(_get_attr(first_choice, "finish_reason", "") or "").lower()
            message = _get_attr(first_choice, "message", None)
            content = _get_attr(message, "content", None)
            content_text = _extract_message_text(content)

            if finish_reason == "length":
                raise TruncatedOutputError("Model output hit max_tokens limit.")

            try:
                return parse_json_array_loose(content_text)
            except ParseModelOutputError as parse_exc:
                if likely_truncated(content_text):
                    raise TruncatedOutputError("Model output appears truncated before JSON completion.") from parse_exc
                raise

        except TruncatedOutputError:
            raise
        except ParseModelOutputError:
            raise
        except Exception as exc:
            last_error = exc
            logger.warning("Attempt %s/%s failed: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                _sleep_backoff(attempt)

    raise RuntimeError(f"Model call failed after {MAX_RETRIES} attempts: {last_error}")


def extract_page_detail_lines(client: Mistral, doc: fitz.Document, page_num: int) -> List[Dict[str, Any]]:
    page_image = render_page_image(doc, page_num, DPI)
    image_b64, media_type = image_to_base64_jpeg(page_image)

    try:
        lines = ask_model_for_image(client, image_b64, media_type, PROMPT)
        if REQUEST_PAUSE_SECONDS > 0:
            time.sleep(REQUEST_PAUSE_SECONDS)
        return lines
    except (TruncatedOutputError, ParseModelOutputError):
        logger.info(
            "Page %s likely exceeded model output limits; retrying with %s segments.",
            page_num + 1,
            PAGE_SEGMENT_FALLBACK_PARTS,
        )

        segmented_rows: List[Dict[str, Any]] = []
        segments = split_image_vertical(page_image, PAGE_SEGMENT_FALLBACK_PARTS, PAGE_SEGMENT_OVERLAP_PX)
        for idx, (segment_img, bounds) in enumerate(segments, start=1):
            seg_b64, seg_media = image_to_base64_jpeg(segment_img)
            seg_prompt = _build_prompt(
                segment_hint=(
                    f"Image segment {idx}/{len(segments)} covering y-pixels {bounds[0]} to {bounds[1]}. "
                    "Extract only lines visible in this segment."
                )
            )
            lines = ask_model_for_image(client, seg_b64, seg_media, seg_prompt)
            segmented_rows.extend(lines)
            if REQUEST_PAUSE_SECONDS > 0:
                time.sleep(REQUEST_PAUSE_SECONDS)

        return dedupe_adjacent_rows(segmented_rows)


def process_checks_to_csv(pdf_folder: str, output_csv: str) -> int:
    _require_runtime_deps()
    pdf_dir = Path(pdf_folder)
    if not pdf_dir.exists() or not pdf_dir.is_dir():
        raise FileNotFoundError(f"PDF folder does not exist: {pdf_folder}")

    pdf_paths = sorted([p for p in pdf_dir.iterdir() if p.suffix.lower() == ".pdf"])
    if not pdf_paths:
        logger.info("No PDF files found in %s.", pdf_folder)
        return 0

    api_key = require_api_key()
    client = Mistral(api_key=api_key)

    total_rows = 0
    logger.info("Found %s PDF file(s). Processing...", len(pdf_paths))

    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_COLUMNS)
        writer.writeheader()

        for pdf_path in pdf_paths:
            logger.info("Processing %s", pdf_path.name)
            file_rows = 0

            try:
                doc = fitz.open(pdf_path)
                try:
                    for page_num in range(doc.page_count):
                        detail_lines = extract_page_detail_lines(client, doc, page_num)
                        for line in detail_lines:
                            writer.writerow(json_row_to_csv_row(line))
                            file_rows += 1
                            total_rows += 1
                        logger.info("Page %s: %s detail line(s)", page_num + 1, len(detail_lines))
                finally:
                    doc.close()

                logger.info("Done %s: %s rows", pdf_path.name, file_rows)

            except Exception as exc:
                logger.error("ERROR in %s: %s", pdf_path.name, exc, exc_info=True)

    logger.info("Done. Wrote %s row(s) to %s", total_rows, output_csv)
    return total_rows

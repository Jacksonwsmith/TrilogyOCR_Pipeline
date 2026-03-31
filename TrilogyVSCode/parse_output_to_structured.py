import csv
import re
import os

CSV_COLUMNS = [
    "Operator ID", "Operator Name", "Owner Name", "Owner Number",
    "Check Number", "Check Date", "Check Amount", "Operator CC",
    "Operator API", "Partner API", "MA API", "Partner CC",
    "Property Description", "Property State", "Property County",
    "Product Code", "Product Description", "Interest Code", "Interest Type",
    "Owner Percent", "Distribution Percent", "Prod Date", "Price",
    "BTU Factor", "Gross Volume", "Gross Value", "Gross Taxes",
    "Gross Deducts", "Net Value", "Owner Gross Volume", "Owner Gross Value",
    "Owner Gross Taxes", "Owner Gross Deducts", "Owner Net Value",
]


def normalize_num(s):
    if s is None:
        return ""
    s = s.strip()
    if s == "":
        return ""
    # Handle parentheses as negative numbers
    negative = False
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1]
    s = s.replace(",", "")
    try:
        v = float(s)
        if negative:
            v = -v
        # Keep as string to preserve formatting in CSV
        return str(v)
    except Exception:
        return s


def parse_property_line(line):
    # Expect: Property: <id> <desc>, TOWN/DISTRICT: ..., State: XX, County: NAME Operator API# - 12345
    # Returns: (description_without_id, state, county, operator_api, operator_cc)
    prop_id = ""
    desc = ""
    state = ""
    county = ""
    operator_api = ""
    operator_cc = ""

    m = re.search(r"Property:\s*(\S+)\s*(.*)", line)
    if m:
        prop_id = m.group(1)
        rest = m.group(2)
        operator_cc = prop_id
        # Try to find State:
        s = re.search(r"State:\s*([^,]+)", rest)
        if s:
            state = s.group(1).strip()
        c = re.search(r"County:\s*([^,\s]+)", rest)
        if c:
            county = c.group(1).strip()
        api = re.search(r"Operator API#\s*-\s*(\d+)", rest)
        if api:
            operator_api = api.group(1)
        # Description is everything up to location metadata such as TOWN/DISTRICT or LATITUDE.
        desc_part = rest.split("State:")[0]
        desc = re.split(r"TOWN/|LATITUDE:", desc_part, maxsplit=1)[0].strip().strip(',')

    return desc, state, county, operator_api, operator_cc


def parse_interest_line(line):
    # Extract the interest label and production date.
    interest_code = ""
    interest_type = ""
    prod_date = ""
    nums = []

    # Clean up multiple spaces
    txt = line.strip()
    # Match a leading label before the production month, e.g.:
    # "ROYALTY INTEREST Nov 25 ..." or "FRR1 Jan 26 ..."
    it_match = re.match(r"^(.+?)\s+([A-Za-z]{3,} \d{1,2})\s+(.*)$", txt)
    if it_match:
        label = it_match.group(1).strip()
        prod_date = it_match.group(2).strip()
        remainder = it_match.group(3).strip()
        if "INTEREST" in label.upper():
            interest_type = label
        else:
            interest_code = label
    else:
        remainder = txt

    # Find numeric tokens (including parentheses)
    nums = re.findall(r"\(?[0-9,]+\.?[0-9]*\)?", remainder)
    nums = [normalize_num(n) for n in nums]

    # Map numbers to columns based on observed report ordering.
    # Observed sequence after the date: [BTU Factor, Gross Volume, Price, Gross Value,
    # Owner Percent, Distribution Percent, Owner Gross Volume, Owner Gross Value]
    row = {
        "Prod Date": prod_date,
        "Interest Type": interest_type,
        "Interest Code": interest_code,
        "BTU Factor": "",
        "Gross Volume": "",
        "Price": "",
        "Gross Value": "",
        "Gross Taxes": "",
        "Gross Deducts": "",
        "Net Value": "",
        "Owner Percent": "",
        "Distribution Percent": "",
        "Owner Gross Volume": "",
        "Owner Gross Value": "",
        "Owner Gross Taxes": "",
        "Owner Gross Deducts": "",
        "Owner Net Value": "",
    }

    if len(nums) == 7:
        row["BTU Factor"] = nums[0]
        row["Gross Volume"] = nums[1]
        row["Price"] = nums[2]
        row["Gross Value"] = "0.0"
        row["Owner Percent"] = nums[3]
        row["Distribution Percent"] = nums[4]
        row["Owner Gross Volume"] = nums[5]
        row["Owner Gross Value"] = nums[6]
    else:
        if len(nums) >= 1:
            row["BTU Factor"] = nums[0]
        if len(nums) >= 2:
            row["Gross Volume"] = nums[1]
        if len(nums) >= 3:
            row["Price"] = nums[2]
        if len(nums) >= 4:
            row["Gross Value"] = nums[3]
        if len(nums) >= 5:
            row["Owner Percent"] = nums[4]
        if len(nums) >= 6:
            row["Distribution Percent"] = nums[5]
        if len(nums) >= 7:
            row["Owner Gross Volume"] = nums[6]
        if len(nums) >= 8:
            row["Owner Gross Value"] = nums[7]

    # Net values: default to gross if no tax/deduct fields present
    if row["Gross Value"] != "":
        row["Net Value"] = row["Gross Value"]
    if row["Owner Gross Value"] != "":
        row["Owner Net Value"] = row["Owner Gross Value"]

    return row


def is_interest_detail_line(text):
    txt = text.strip()
    return bool(re.match(r"^.+?\s+[A-Z][a-z]{2}\s+\d{2}\s+\(?[0-9,]+\.?[0-9]*\)?", txt))


def main(input_csv="output.csv", output_csv="output_structured.csv"):
    if not os.path.exists(input_csv):
        print(f"Input CSV not found: {input_csv}")
        return

    with open(input_csv, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        lines = list(reader)

    out_rows = []

    # Current metadata
    meta = {k: "" for k in [
        "Operator ID", "Operator Name", "Owner Name", "Owner Number",
        "Check Number", "Check Date", "Check Amount", "Operator CC",
        "Operator API", "Partner API", "MA API", "Partner CC",
    ]}

    curr_property_desc = ""
    curr_property_state = ""
    curr_property_county = ""

    for entry in lines:
        text = entry.get('line', '').strip()
        if not text:
            continue

        # Header with Check Number, Owner, Operator
        if 'Check Number' in text and 'Owner' in text and 'Operator' in text:
            m = re.search(r'Check Number\s*(\d+)\s*Owner\s*(\d+)\s*(.*?)\s*Operator\s*(.*)', text)
            if m:
                meta['Check Number'] = m.group(1).strip()
                meta['Owner Number'] = m.group(2).strip()
                meta['Owner Name'] = m.group(3).strip()
                meta['Operator Name'] = m.group(4).strip()
                continue

        # Alternative header form: "1064941 DIVERSIFIED PRODUCTION, LLC Check Number 5001502951"
        m2 = re.search(r'^(\d{4,})\s+(.+?)\s+Check Number\s+(\d+)', text)
        if m2:
            meta['Owner Number'] = m2.group(1).strip()
            meta['Operator Name'] = m2.group(2).strip()
            meta['Check Number'] = m2.group(3).strip()
            continue

        # Check Amount
        if 'Check Amount' in text:
            m = re.search(r'Check Amount\s*([\d,]+\.?\d*)', text)
            if m:
                meta['Check Amount'] = normalize_num(m.group(1))
            # Owner name sometimes in same line before 'Check Amount'
            owner_match = re.match(r'^(.*?)\s+Check Amount', text)
            if owner_match and not meta['Owner Name']:
                meta['Owner Name'] = owner_match.group(1).strip()
            continue

        # Check Date
        if 'Check Date' in text:
            m = re.search(r'Check Date\s*(.*)', text)
            if m:
                meta['Check Date'] = m.group(1).strip()
            continue

        # Property line
        if text.startswith('Property:'):
            desc, state, county, op_api, op_cc = parse_property_line(text)
            curr_property_desc = desc
            curr_property_state = state
            curr_property_county = county
            if op_api:
                meta['Operator API'] = op_api
            if op_cc:
                meta['Operator CC'] = op_cc
            continue

        # Interest lines (ROYALTY INTEREST...)
        if is_interest_detail_line(text):
            parsed = parse_interest_line(text)
            out = {col: "" for col in CSV_COLUMNS}
            # copy meta
            for k in meta:
                if k in out:
                    out[k] = meta[k]
            out['Property Description'] = curr_property_desc
            out['Property State'] = curr_property_state
            out['Property County'] = curr_property_county
            out['Interest Type'] = parsed.get('Interest Type', '')
            out['Prod Date'] = parsed.get('Prod Date', '')
            out['Interest Code'] = parsed.get('Interest Code', '')
            out['BTU Factor'] = parsed.get('BTU Factor', '')
            out['Gross Volume'] = parsed.get('Gross Volume', '')
            out['Price'] = parsed.get('Price', '')
            out['Gross Value'] = parsed.get('Gross Value', '')
            out['Net Value'] = parsed.get('Net Value', '')
            out['Owner Percent'] = parsed.get('Owner Percent', '')
            out['Distribution Percent'] = parsed.get('Distribution Percent', '')
            out['Owner Gross Volume'] = parsed.get('Owner Gross Volume', '')
            out['Owner Gross Value'] = parsed.get('Owner Gross Value', '')
            out['Owner Net Value'] = parsed.get('Owner Net Value', '')

            out_rows.append(out)
            continue

        # Totals lines, skip or could be used

    # Write output CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for r in out_rows:
            writer.writerow(r)

    print(f"Wrote {len(out_rows)} rows to {output_csv}")


if __name__ == '__main__':
    main()

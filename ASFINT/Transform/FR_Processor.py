import pandas as pd
import re
from datetime import datetime
import argparse
from ASFINT.Utility.Utils import heading_finder
from ASFINT.Utility.Cleaning import in_df

def _row_has(tokens, row) -> bool:
    """Return True if all tokens are present in the given row (as strings)."""
    s = set(str(x).strip() for x in row.tolist())
    return all(t in s for t in tokens)

def _promote_header(block: pd.DataFrame, header_row_idx: int) -> pd.DataFrame:
    """Use the values of header_row_idx as column names and return rows beneath it."""
    header = block.loc[header_row_idx].astype(str).str.strip().tolist()
    out = block.loc[header_row_idx + 1 :].copy()
    out.columns = header
    out = out.reset_index(drop=True)
    return out

def _sanitize_date_for_filename(date_str: str) -> str:
    return str(date_str).replace("/", "-").replace("\\", "-").replace(":", "-")

def FR_Helper(df):
    """
    Split raw FR sheet into two DataFrames:
      - requests_df: contains Amount Requested
      - decisions_df: contains Committee Status + Amount Approved
    Both are cropped starting at 'Appx' and filtered by allowed FY24 alphabet.
    """
    # Find starting point (first "Appx")
    start_idx = df.index[df.iloc[:, 0].astype(str).str.contains("Appx", na=False)]
    if len(start_idx) == 0:
        return df, None, None
    start = start_idx[0]

    # Allowed labels (A-Z, AA-AZ, BB-ZZ)
    allowed = set(
        [chr(c) for c in range(65, 91)] +
        [f"A{chr(c)}" for c in range(65, 91)] +
        [f"B{chr(c)}" for c in range(65, 91)]
    )

    # Crop to rows after "Appx"
    cropped = df.iloc[start + 1:].copy()
    cropped = cropped[cropped.iloc[:, 0].astype(str).isin(allowed)]

    # Now split into two subtables: requests vs committee decisions
    # Assumption: the raw file stacks two tables vertically with same headers
    header_row = cropped.columns.tolist()
    if "Amount Requested" in header_row and "Committee Status" in header_row:
        # Already unified, rare case
        return cropped, None, None

    # Otherwise, detect split by column names
    request_cols = [c for c in cropped.columns if "Requested" in str(c)]
    decision_cols = [c for c in cropped.columns if "Approved" in str(c) or "Committee" in str(c)]

    if not request_cols or not decision_cols:
        # Could not split
        return cropped, None, None

    requests_df = cropped.loc[:, [c for c in cropped.columns if "Requested" in str(c) or c in ["Appx", "Org Name", "Request Type", "Org Type", "Funding Source", "Primary Contact", "Email Address"]]]
    decisions_df = cropped.loc[:, [c for c in cropped.columns if "Approved" in str(c) or "Committee" in str(c) or c in ["Appx", "Org Name"]]]

    return cropped, requests_df, decisions_df

def FR_ProcessorV2(df: pd.DataFrame, txt: str, date_format: str, original_filename: str = None):
    """
    Merge FR sheet's two stacked tables by ['Appx.', 'Org Name']:
      - Table 1 is preserved (columns/values untouched)
      - Table 2 contributes 'Amount' and 'Committee Status'
    Output columns (when present):
      ['Appx.', 'Org Name', 'Request Type', 'Org Type (year)',
       'Amount Requested', 'Amount', 'Committee Status',
       'Funding Source', 'Primary Contact', 'Email Address', 'Date']

    Args:
        df: Input DataFrame
        txt: Companion text (used for date extraction if original_filename not provided)
        date_format: Date format string (e.g., "%m/%d/%Y")
        original_filename: Optional original filename. If provided, output will be named
                          "{original_filename} Cleaned" instead of "FR_clean_{date}"

    Returns:
        Dict[str, pd.DataFrame]: Dictionary with single key-value pair {out_name: processed_df}
    """
    # 1) Extract date from the input data
    # The date can appear in row 1 OR row 2 in format: "YYYY-MM-DD Finance Committee Agenda and Minutes"
    # Some files have a blank row 1, others start with the date in row 1
    extracted_date = None
    if df is not None and not df.empty:
        # Check row 1 (index 0) and row 2 (index 1) for date pattern
        rows_to_check = []
        if len(df) > 0:
            rows_to_check.append(df.iloc[0])
        if len(df) > 1:
            rows_to_check.append(df.iloc[1])

        for row in rows_to_check:
            row_values = row.astype(str).tolist()
            for val in row_values:
                # Look for YYYY-MM-DD pattern
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', val)
                if date_match:
                    try:
                        # Parse and reformat to the desired format (e.g., MM/DD/YYYY)
                        date_obj = datetime.strptime(date_match.group(1), "%Y-%m-%d")
                        extracted_date = date_obj.strftime(date_format)
                        break
                    except ValueError:
                        continue
            if extracted_date:
                break

    # if prev case doesnt work: try to extract from txt parameter
    if extracted_date is None and txt:
        m = re.search(r"(\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2})", str(txt))
        if m:
            try:
                # Try to parse the found date
                date_str = m.group(1)
                if "/" in date_str:
                    date_obj = datetime.strptime(date_str, "%m/%d/%Y")
                else:
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                extracted_date = date_obj.strftime(date_format)
            except ValueError:
                pass

    # If still no date found, use "undated"
    if extracted_date is None:
        extracted_date = "undated"

    # 2) determine output name
    if original_filename:
        # Use original filename with "Cleaned" suffix
        # Remove common file extensions and clean up
        base_name = original_filename.replace(".csv", "").replace(".xlsx", "").replace(".gsheet", "")
        base_name = re.sub(r"\([\d]+\)", "", base_name).strip()  # Remove (1), (2), etc.
        base_name = base_name.replace(" - Sheet1", "").replace("- Sheet1", "").strip()
        out_name = f"{base_name} Cleaned"
    else:
        # Fall back to date-based naming
        safe_date = _sanitize_date_for_filename(extracted_date)
        out_name = f"FR_clean_{safe_date}"

    if df is None or df.empty:
        return {out_name: pd.DataFrame()}

    # 2) start scan near first "Appx" occurrence in first column
    start_idx = 0
    for i in range(min(len(df), 200)):
        v = str(df.iloc[i, 0])
        if "Appx" in v:
            start_idx = i
            break
    sheet = df.iloc[start_idx:].reset_index(drop=True)

    # 3) detect header rows for table1 (requests) and table2 (decisions)
    t1_hdr = t2_hdr = None
    scan_limit = min(100, len(sheet))
    for i in range(scan_limit):
        row = sheet.iloc[i]
        if t1_hdr is None and _row_has(["Appx.", "Org Name", "Amount Requested"], row):
            t1_hdr = i
            continue
        # table2 header can vary; accept either Committee Status or Amount
        if _row_has(["Appx.", "Org Name", "Committee Status"], row) or _row_has(["Appx.", "Org Name", "Amount"], row):
            t2_hdr = i
            if t1_hdr is not None and t2_hdr > t1_hdr:
                break

    # if we can't find table headers, return the sheet as-is
    if t1_hdr is None:
        return {out_name: sheet}

    # if there's only one table (no table2), promote table1's header and return it
    if t2_hdr is None or t2_hdr <= t1_hdr:
        table1 = _promote_header(sheet, t1_hdr)
        # Normalize header variants
        if "Appx" in table1.columns and "Appx." not in table1.columns:
            table1.rename(columns={"Appx": "Appx."}, inplace=True)
        if "Org Type" in table1.columns and "Org Type (year)" not in table1.columns:
            table1.rename(columns={"Org Type": "Org Type (year)"}, inplace=True)
        if "Email" in table1.columns and "Email Address" not in table1.columns:
            table1.rename(columns={"Email": "Email Address"}, inplace=True)
        # Add Date column
        table1['Date'] = extracted_date
        return {out_name: table1}

    # 4) promote headers and slice blocks
    table1 = _promote_header(sheet, t1_hdr)
    cutoff = max(0, t2_hdr - t1_hdr - 1)
    if cutoff > 0:
        table1 = table1.iloc[:cutoff].copy()

    table2 = _promote_header(sheet, t2_hdr)

    # 5) normalize some header variants for joining/selection
    # "Appx" variants
    for tbl in (table1, table2):
        if "Appx" in tbl.columns and "Appx." not in tbl.columns:
            tbl.rename(columns={"Appx": "Appx."}, inplace=True)
        if "Org Type" in tbl.columns and "Org Type (year)" not in tbl.columns:
            tbl.rename(columns={"Org Type": "Org Type (year)"}, inplace=True)
        # sometimes 'Email' instead of 'Email Address'
        if "Email" in tbl.columns and "Email Address" not in tbl.columns:
            tbl.rename(columns={"Email": "Email Address"}, inplace=True)
        # amount column on table2 could be 'Amount Approved' -> map to 'Amount'
        if "Amount Approved" in tbl.columns and "Amount" not in tbl.columns:
            tbl.rename(columns={"Amount Approved": "Amount"}, inplace=True)

    # 6) build the left (table1) exactly as user wants to preserve
    left_keep = ["Appx.", "Org Name", "Request Type", "Org Type (year)",
                 "Amount Requested", "Funding Source", "Primary Contact", "Email Address"]
    left = table1[[c for c in left_keep if c in table1.columns]].copy()

    # 7) build the right (table2) with only the two fields we want to add
    right_keep = ["Appx.", "Org Name", "Amount", "Committee Status"]
    right = table2[[c for c in right_keep if c in table2.columns]].copy()

    # 8) merge (left-join; do not alter left values)
    join_keys = [k for k in ["Appx.", "Org Name"] if k in left.columns and k in right.columns]
    if len(join_keys) < 2:
        merged = left  # cannot join reliably; return table1 only
    else:
        merged = left.merge(right, on=join_keys, how="left")

    # 9) Add Date column to the merged data
    merged['Date'] = extracted_date

    # 10) final column order (including Date at the end)
    final_cols = ["Appx.", "Org Name", "Request Type", "Org Type (year)",
                  "Amount Requested", "Amount", "Committee Status",
                  "Funding Source", "Primary Contact", "Email Address", "Date"]
    final = merged[[c for c in final_cols if c in merged.columns]].copy()

    return {out_name: final}


def FR_ProcessorV2_Multi(dfs_with_txt: list, date_format: str = "%Y-%m-%d", original_filenames: list = None):
    """
    Process multiple FR files independently and return separate outputs for each.

    Args:
        dfs_with_txt: List of tuples [(df1, txt1), (df2, txt2), ...]
        date_format: Format string for dates in output filenames
        original_filenames: Optional list of original filenames. If provided, outputs will be named
                           "{original_filename} Cleaned" instead of "FR_clean_{date}"

    Returns:
        Dict[str, pd.DataFrame]: Dictionary with multiple key-value pairs, one per input file
                                 {filename1: processed_df1, filename2: processed_df2, ...}

    Example:
        >>> inputs = [(df1, "Report 01/15/2024"), (df2, "Report 02/20/2024")]
        >>> results = FR_ProcessorV2_Multi(inputs)
        >>> # Results: {"FR_clean_2024-01-15": df1_processed, "FR_clean_2024-02-20": df2_processed}
        >>>
        >>> # With original filenames:
        >>> results = FR_ProcessorV2_Multi(inputs, original_filenames=["FR 24_25 F1", "FR 24_25 F2"])
        >>> # Results: {"FR 24_25 F1 Cleaned": df1_processed, "FR 24_25 F2 Cleaned": df2_processed}
    """
    all_results = {}

    for idx, pair in enumerate(dfs_with_txt):
        if not isinstance(pair, (tuple, list)) or len(pair) != 2:
            raise ValueError(f"Expected (df, txt) tuple at index {idx}, got {type(pair)}")

        df, txt = pair

        if not isinstance(df, pd.DataFrame):
            raise ValueError(f"Expected pandas DataFrame at index {idx}, got {type(df)}")

        # Get original filename if provided
        orig_name = original_filenames[idx] if original_filenames and idx < len(original_filenames) else None

        # Process each file individually
        result = FR_ProcessorV2(df, txt, date_format, original_filename=orig_name)

        # Merge into the combined results dictionary
        all_results.update(result)

    return all_results
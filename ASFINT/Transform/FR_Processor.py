import pandas as pd
import re
from datetime import datetime
import argparse
from ASFINT.Utility.Utils import heading_finder
from ASFINT.Utility.Cleaning import in_df

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

def FR_ProcessorV2(df, txt, date_format):
    """
    Clean FR sheet:
      - Extract meeting date from text
      - Split into requests & decisions
      - Merge on Appx + Org Name
      - Output unified DataFrame with Amount Requested + Amount Allowed
    """
    # Extract date from text
    date_regex = r"(\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2})"
    match = re.search(date_regex, str(txt))
    date_str = match.group(1) if match else "undated"

    cropped, requests_df, decisions_df = FR_Helper(df)

    if requests_df is None or decisions_df is None:
        # Fallback: just return cropped
        return {"FR_clean_" + date_str: cropped}

    # Merge on keys
    merged = pd.merge(
        requests_df,
        decisions_df,
        on=["Appx", "Org Name"],
        how="left",
        suffixes=("", "_dec")
    )

    # Rename columns
    if in_df(merged, "Amount Approved"):
        merged = merged.rename(columns={"Amount Approved": "Amount Allowed"})
    if in_df(merged, "Committee Status"):
        merged = merged.drop(columns=["Committee Status"])

    # Reorder columns
    col_order = [
        "Appx", "Org Name", "Request Type", "Org Type",
        "Amount Requested", "Amount Allowed", "Funding Source",
        "Primary Contact", "Email Address"
    ]
    merged = merged[[c for c in col_order if c in merged.columns]]

    safe_date = str(date_str).replace("/", "-").replace("\\", "-").replace(":", "-")
    out_name = f"FR_clean_{safe_date}"
    return {out_name: merged}

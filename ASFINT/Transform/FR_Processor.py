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

    def in_alphabet_helper(series: pd.Series, alphabet_list: list[str], nth: int = 1) -> pd.Series:
        occurrences = dict.fromkeys(alphabet_list, 0)
        result_mask = []
        for val in series:
            if val in occurrences:
                if occurrences[val] < nth:
                    result_mask.append(True)
                    occurrences[val] += 1
                else:
                    result_mask.append(False)
            else:
                result_mask.append(False)
        return pd.Series(result_mask, index=series.index)

    if end_col is not None:
        assert isinstance(end_col, str) or isinstance(end_col, int), "'end_col' must be index of column or name of column."
        assert in_df(end_col, df), 'Given end_col is not in the given df.'
    else:
        end_col = start_col

    start_col_index = df.columns.get_loc(start_col) if isinstance(start_col, str) else start_col #extract index of start and end column
    end_col_index = df.columns.get_loc(end_col) if isinstance(end_col, str) else end_col #extract index of start and end column
    
    copy = df.copy()
    copy = heading_finder(copy, start_col=0, start=given_start, start_logic='contains') # no ending logic just take all rows below the starting point
    col = copy.columns[start_col_index]
    try:
        if alphabet is None:
            na_indices = copy[copy[col].isna()].index
            if na_indices.empty:
                raise ValueError("No NaN row found to mark as end of section.")
            ending_row_index = na_indices[0]
        else:
            mask = in_alphabet_helper(copy[col], alphabet.copy(), nth_occurence)
            valid_rows: pd.DataFrame = copy[mask]
            if valid_rows.empty:
                raise ValueError("No valid rows with alphabet keys found.")
            ending_row_index = valid_rows.index[-1] + 1

    except Exception as e:
           print(f"Warning: Could not insert ending keyword '{adding_end_keyword}' in column {end_col}, received exception\n{e}")
    rv = copy[copy.index < ending_row_index]
    return rv

def FR_ProcessorV2(df: pd.DataFrame, txt: str, date_format="%m-%d-%Y", debug=False, reporting=False): # make sure that we spit out dates formatted with dashes not slashes distinguishing d, m, y
    """Employs heading_finder to clean data. Takes in the same spreadsheet as a dataframe (to clean) and txt (to search for the date) then returns the relevant info"""
    assert isinstance(df, pd.DataFrame), f'Inputted df is not a dataframe but type {type(df)}'

    FY24_Alphabet = 'A B C D E F G H I J K L M N O P Q R S T U V W X Y Z'.split()
    FY24_Alphabet.extend(
        'AA AB AC AD AE AF AG AH AI AJ AK AL AM AN AO AP AQ AR AS AT AU AV AW AX AY AZ'.split()
    )
    FY24_Alphabet.extend(
        'BB CC DD EE FF GG HH II JJ KK LL MM NN OO PP QQ RR SS TT UU VV WW XX YY ZZ'.split()
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

def FR_ProcessorV2(df: pd.DataFrame, txt: str, date_format: str):
    """
    Merge FR sheet's two stacked tables by ['Appx.', 'Org Name']:
      - Table 1 is preserved (columns/values untouched)
      - Table 2 contributes 'Amount' and 'Committee Status'
    Output columns (when present):
      ['Appx.', 'Org Name', 'Request Type', 'Org Type (year)',
       'Amount Requested', 'Amount', 'Committee Status',
       'Funding Source', 'Primary Contact', 'Email Address']
    """
    # 1) name from date in companion text
    m = re.search(r"(\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2})", str(txt) or "")
    safe_date = _sanitize_date_for_filename(m.group(1) if m else "undated")
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

    # if we can't confidently split, just return the visible portion as-is
    if t1_hdr is None or t2_hdr is None or t2_hdr <= t1_hdr:
        return {out_name: sheet}

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
        date_str = date_match[0]  # the matched date string
        dt = pd.to_datetime(date_str, errors='coerce')  # parse string into timestamp object
        date = dt.strftime(date_format)
    try:
        rv = FR_Helper(df, alphabet=FY24_Alphabet, reporting=reporting) # added reporting call
    except Exception as e:
        if debug:
            print(f"FR_ProcessorV2 errored on df\n{df}")
        raise e
    return rv, date
    

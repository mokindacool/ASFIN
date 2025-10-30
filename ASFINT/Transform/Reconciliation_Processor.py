"""
Reconciliation_Processor.py

Reconciles FR (Finance Resolution) outputs with Agenda outputs.
The Agenda output is treated as the "source of truth" and will override
FR data when there are conflicts.

Key behavior:
- Merges on 'Org Name' column
- Agenda's 'Amount' and 'Committee Status' override FR's values when both exist
- Includes organizations from both sources (union of all orgs)
"""

import pandas as pd
import numpy as np
from typing import Tuple, Dict
from pathlib import Path


def Reconcile_FR_Agenda(fr_df: pd.DataFrame, agenda_df: pd.DataFrame) -> pd.DataFrame:
    """
    Reconcile FR and Agenda outputs, with Agenda data taking precedence.

    Args:
        fr_df: DataFrame from FR processor with columns:
               ['Org Name', 'Request Type', 'Amount', 'Committee Status', ...]
        agenda_df: DataFrame from Agenda processor with columns:
                   ['Org Name', 'Request Type', 'Amount', 'Committee Status', 'Date']

    Returns:
        Reconciled DataFrame with Agenda values overriding FR values where conflicts exist.

    Example:
        FR:     Helix @ Berkeley | Contingency | NaN | Tabled indefinitely
        Agenda: Helix @ Berkeley | Contingency | 100 | Approved
        Result: Helix @ Berkeley | Contingency | 100 | Approved (Agenda wins)
    """

    # check if input is there
    if fr_df is None or fr_df.empty:
        print("[RECONCILE] FR DataFrame is empty, returning Agenda data only")
        return agenda_df.copy() if agenda_df is not None else pd.DataFrame()

    if agenda_df is None or agenda_df.empty:
        print("[RECONCILE] Agenda DataFrame is empty, returning FR data only")
        return fr_df.copy()

    # Ensure 'Org Name' column exists in both
    if 'Org Name' not in fr_df.columns:
        raise ValueError("FR DataFrame missing 'Org Name' column")
    if 'Org Name' not in agenda_df.columns:
        raise ValueError("Agenda DataFrame missing 'Org Name' column")

    # Clean org names for matching (strip whitespace, normalize)
    fr_df = fr_df.copy()
    agenda_df = agenda_df.copy()

    fr_df['Org Name'] = fr_df['Org Name'].astype(str).str.strip()
    agenda_df['Org Name'] = agenda_df['Org Name'].astype(str).str.strip()

    # Prepare FR data: select relevant columns
    fr_cols = ['Org Name', 'Request Type']
    if 'Amount Requested' in fr_df.columns:
        fr_cols.append('Amount Requested')
    if 'Org Type (year)' in fr_df.columns:
        fr_cols.append('Org Type (year)')

    # Add FR's Amount and Committee Status with suffix to track source
    if 'Amount' in fr_df.columns:
        fr_cols.append('Amount')
    if 'Committee Status' in fr_df.columns:
        fr_cols.append('Committee Status')

    fr_subset = fr_df[[col for col in fr_cols if col in fr_df.columns]].copy()

    # Prepare Agenda data: select relevant columns with suffix
    agenda_subset = agenda_df[['Org Name', 'Request Type', 'Amount', 'Committee Status', 'Date']].copy()

    # Rename Agenda columns to mark them as agenda source
    agenda_subset = agenda_subset.rename(columns={
        'Amount': 'Agenda_Amount',
        'Committee Status': 'Agenda_Status',
        'Request Type': 'Agenda_Request_Type',
        'Date': 'Agenda_Date'
    })

    # Merge: outer join to include all organizations from both sources
    merged = fr_subset.merge(
        agenda_subset,
        on='Org Name',
        how='outer',
        indicator=True
    )

    # Apply reconciliation logic: Agenda overrides FR
    # Priority: Agenda > FR

    # Amount reconciliation
    if 'Amount' in merged.columns:
        merged['Amount_Final'] = merged['Agenda_Amount'].combine_first(merged['Amount'])
    else:
        merged['Amount_Final'] = merged['Agenda_Amount']

    # Committee Status reconciliation
    if 'Committee Status' in merged.columns:
        merged['Committee_Status_Final'] = merged['Agenda_Status'].combine_first(merged['Committee Status'])
    else:
        merged['Committee_Status_Final'] = merged['Agenda_Status']

    # Request Type: prefer Agenda if available, otherwise use FR
    if 'Request Type' in merged.columns:
        merged['Request_Type_Final'] = merged['Agenda_Request_Type'].combine_first(merged['Request Type'])
    else:
        merged['Request_Type_Final'] = merged['Agenda_Request_Type']

    # Add source tracking column
    merged['Data_Source'] = merged['_merge'].map({
        'left_only': 'FR Only',
        'right_only': 'Agenda Only',
        'both': 'Both (Agenda Priority)'
    })

    # Apply business rule: Set Amount based on Committee Status and Request Type
    # - If not "Approved": set Amount to 0
    # - EXCEPT for Sponsorships that are Tabled/Denied: leave Amount as empty (NaN)
    merged['Committee_Status_Final'] = merged['Committee_Status_Final'].fillna('')
    is_approved = merged['Committee_Status_Final'].astype(str).str.contains('Approved', case=False, na=False)

    # For non-approved items, set Amount to 0
    merged.loc[~is_approved, 'Amount_Final'] = 0

    # Special case: For Sponsorships that are Tabled or Denied, use empty (NaN) instead of 0
    is_sponsorship = merged['Request_Type_Final'].astype(str).str.contains('Sponsorship', case=False, na=False)
    is_tabled_or_denied = (
        merged['Committee_Status_Final'].astype(str).str.contains('Table', case=False, na=False) |
        merged['Committee_Status_Final'].astype(str).str.contains('Denied', case=False, na=False)
    )
    merged.loc[is_sponsorship & is_tabled_or_denied, 'Amount_Final'] = None

    # Build final output DataFrame with exact column order
    # Column order: Org Name, Request Type, Amount Requested, Amount, Committee Status, Source, Org Type (year), Date
    final_cols_ordered = []
    final_col_mapping = {}

    # Required columns in exact order
    final_cols_ordered.append('Org Name')
    final_col_mapping['Org Name'] = 'Org Name'

    final_cols_ordered.append('Request_Type_Final')
    final_col_mapping['Request_Type_Final'] = 'Request Type'

    # Amount Requested (from FR)
    if 'Amount Requested' in merged.columns:
        final_cols_ordered.append('Amount Requested')
        final_col_mapping['Amount Requested'] = 'Amount Requested'

    final_cols_ordered.append('Amount_Final')
    final_col_mapping['Amount_Final'] = 'Amount'

    final_cols_ordered.append('Committee_Status_Final')
    final_col_mapping['Committee_Status_Final'] = 'Committee Status'

    final_cols_ordered.append('Data_Source')
    final_col_mapping['Data_Source'] = 'Source'

    # Org Type (year) (from FR)
    if 'Org Type (year)' in merged.columns:
        final_cols_ordered.append('Org Type (year)')
        final_col_mapping['Org Type (year)'] = 'Org Type (year)'

    # Date (from Agenda)
    if 'Agenda_Date' in merged.columns:
        final_cols_ordered.append('Agenda_Date')
        final_col_mapping['Agenda_Date'] = 'Date'

    # Select columns that exist and rename them
    result = merged[[col for col in final_cols_ordered if col in merged.columns]].copy()
    result.columns = [final_col_mapping[col] for col in result.columns]

    # Sort by Request Type (custom order), then by Org Name
    # Define the priority order for Request Types
    request_type_order = {
        'Contingency': 1,
        'Finance Rule': 2,
        'Space Reservation': 3,
        'Sponsorship': 4
    }

    # Create a sort key column based on the defined order
    # Any Request Type not in the order dict gets a high number (sorted last)
    result['_sort_key'] = result['Request Type'].map(request_type_order).fillna(999)

    # Sort by Request Type order first, then by Org Name
    result = result.sort_values(['_sort_key', 'Org Name']).reset_index(drop=True)

    # Drop the temporary sort key column
    result = result.drop(columns=['_sort_key'])

    # Log reconciliation summary
    both_count = (merged['_merge'] == 'both').sum()
    fr_only_count = (merged['_merge'] == 'left_only').sum()
    agenda_only_count = (merged['_merge'] == 'right_only').sum()

    print(f"[RECONCILE] Summary:")
    print(f"  - Organizations in both FR and Agenda: {both_count} (Agenda data used)")
    print(f"  - Organizations only in FR: {fr_only_count}")
    print(f"  - Organizations only in Agenda: {agenda_only_count}")
    print(f"  - Total organizations: {len(result)}")

    return result


def _scan_outputs_by_date(output_dir: str) -> Dict[str, Dict[str, Tuple[str, pd.DataFrame]]]:
    """
    Helper function: Scan output directory and group FR/Agenda files by their Date column.

    Args:
        output_dir: Path to directory containing processed CSV files

    Returns:
        Dictionary grouped by date:
        {
            "01/27/2025": {
                "fr": ("FR 24_25 S1 Cleaned", DataFrame),
                "agenda": ("2025-01-27 Agenda", DataFrame)
            },
            "02/03/2025": {...},
            ...
        }
    """
    output_path = Path(output_dir)

    if not output_path.exists():
        raise FileNotFoundError(f"Output directory does not exist: {output_dir}")

    if not output_path.is_dir():
        raise ValueError(f"Expected directory path, got file: {output_dir}")

    # Dictionary to store files grouped by date
    files_by_date = {}

    print(f"\n[BATCH RECONCILE] Scanning {output_dir} for FR and Agenda files...")

    # Scan all CSV files
    for csv_file in output_path.glob("*.csv"):
        try:
            df = pd.read_csv(csv_file)

            # Check if this file has a Date column
            if 'Date' not in df.columns:
                print(f"  [SKIP] {csv_file.name} - no Date column")
                continue

            # Get the date from first row
            date = df['Date'].iloc[0]

            if pd.isna(date) or str(date).strip() == "" or str(date) == "undated":
                print(f"  [SKIP] {csv_file.name} - invalid date: {date}")
                continue

            # Initialize date entry if needed
            if date not in files_by_date:
                files_by_date[date] = {'fr': None, 'agenda': None}

            # Categorize as FR or Agenda based on filename
            filename_stem = csv_file.stem  # filename without extension

            if 'Cleaned' in csv_file.name and ('FR' in csv_file.name or 'fr' in csv_file.name.lower()):
                files_by_date[date]['fr'] = (filename_stem, df)
                print(f"  [FR]     {csv_file.name} → Date: {date}")
            elif 'Agenda' in csv_file.name:
                files_by_date[date]['agenda'] = (filename_stem, df)
                print(f"  [AGENDA] {csv_file.name} → Date: {date}")
            else:
                print(f"  [SKIP] {csv_file.name} - doesn't match FR or Agenda pattern")

        except Exception as e:
            print(f"  [ERROR] {csv_file.name} - {e}")
            continue

    return files_by_date


def Reconcile_FR_Agenda_Batch(
    output_dir: str,
    return_unmatched: bool = False,
    reporting: bool = True
) -> Dict[str, pd.DataFrame]:
    """
    Batch reconcile all FR and Agenda files by matching their Date columns.

    This function scans the output directory for all FR (Cleaned) and Agenda CSV files,
    automatically matches them by date, and reconciles each matched pair.

    Args:
        output_dir: Directory containing FR Cleaned and Agenda CSV files
        return_unmatched: If True, include unmatched FR/Agenda files in output (default: False)
        reporting: If True, print detailed progress logs (default: True)

    Returns:
        Dict mapping output names to reconciled DataFrames
        e.g., {"FR 24_25 S1 Finalized": reconciled_df1, "FR 24_25 S2 Finalized": reconciled_df2}

    Raises:
        FileNotFoundError: If output_dir doesn't exist
        ValueError: If no valid FR/Agenda pairs found

    Example:
        >>> results = Reconcile_FR_Agenda_Batch("files/output/")
        >>> for name, df in results.items():
        ...     print(f"Reconciled: {name}, rows: {len(df)}")
    """
    # Scan and group files by date
    files_by_date = _scan_outputs_by_date(output_dir)

    if not files_by_date:
        raise ValueError(f"No FR or Agenda files with Date columns found in {output_dir}")

    # Track statistics
    reconciled_count = 0
    fr_only_count = 0
    agenda_only_count = 0

    # Store results
    results = {}

    print("\n" + "=" * 70)
    print("BATCH RECONCILIATION - MATCHING FILES BY DATE")
    print("=" * 70)

    # Process each date
    for date in sorted(files_by_date.keys()):
        files = files_by_date[date]
        fr_pair = files.get('fr')
        agenda_pair = files.get('agenda')

        print(f"\nDate: {date}")
        print("-" * 70)

        # Both FR and Agenda exist - reconcile them
        if fr_pair and agenda_pair:
            fr_name, fr_df = fr_pair
            agenda_name, agenda_df = agenda_pair

            print(f"  ✓ FR:     {fr_name}.csv ({len(fr_df)} rows)")
            print(f"  ✓ Agenda: {agenda_name}.csv ({len(agenda_df)} rows)")
            print(f"  → Reconciling...")

            try:
                # Reconcile using existing single-pair function
                reconciled_df = Reconcile_FR_Agenda(fr_df, agenda_df)

                # Generate output name: replace "Cleaned" with "Finalized"
                if "Cleaned" in fr_name:
                    output_name = fr_name.replace("Cleaned", "Finalized")
                else:
                    output_name = f"{fr_name} Finalized"

                results[output_name] = reconciled_df
                reconciled_count += 1

                print(f"  ✓ Success: {output_name}.csv ({len(reconciled_df)} rows)")

            except Exception as e:
                print(f"  ✗ Reconciliation failed: {e}")

        # Only FR exists
        elif fr_pair and not agenda_pair:
            fr_name, fr_df = fr_pair
            print(f"  ✗ FR:     {fr_name}.csv ({len(fr_df)} rows)")
            print(f"  ✗ Agenda: MISSING")
            print(f"  → Cannot reconcile (no matching Agenda file)")
            fr_only_count += 1

            if return_unmatched:
                results[fr_name] = fr_df

        # Only Agenda exists
        elif agenda_pair and not fr_pair:
            agenda_name, agenda_df = agenda_pair
            print(f"  ✗ FR:     MISSING")
            print(f"  ✗ Agenda: {agenda_name}.csv ({len(agenda_df)} rows)")
            print(f"  → Cannot reconcile (no matching FR file)")
            agenda_only_count += 1

            if return_unmatched:
                results[agenda_name] = agenda_df

    # Print summary
    print("\n" + "=" * 70)
    print("BATCH RECONCILIATION SUMMARY")
    print("=" * 70)
    print(f"Total dates found: {len(files_by_date)}")
    print(f"  ✓ Successfully reconciled: {reconciled_count} pairs")
    print(f"  ✗ FR only (no Agenda):     {fr_only_count}")
    print(f"  ✗ Agenda only (no FR):     {agenda_only_count}")
    print(f"\nTotal output files: {len(results)}")
    print("=" * 70)

    if reconciled_count == 0:
        print("\n⚠ WARNING: No files were reconciled!")
        print("  - Make sure both FR and Agenda files have Date columns")
        print("  - Make sure dates match between FR and Agenda files")
        print("  - Reprocess FR files if they're missing Date columns")

    return results

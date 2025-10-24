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
from typing import Tuple


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

    # Validate inputs
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

    # Apply business rule: Set Amount to 0 if Committee Status is not "Approved"
    # Check if status contains "Approved" (case-insensitive)
    merged['Committee_Status_Final'] = merged['Committee_Status_Final'].fillna('')
    is_approved = merged['Committee_Status_Final'].astype(str).str.contains('Approved', case=False, na=False)
    merged.loc[~is_approved, 'Amount_Final'] = 0

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

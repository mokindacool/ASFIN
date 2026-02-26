# Reconciliation Processor

## Overview

The Reconciliation Processor merges FR (Finance Resolution) and Agenda outputs, using Agenda data as the "source of truth" when conflicts exist.

## Purpose

When processing Finance Committee data:
- **FR Processor** extracts data from Finance Resolution spreadsheets
- **Agenda Processor** extracts data from meeting agenda/minutes text files

Sometimes these sources contain conflicting information. For example:
- FR shows: "Helix @ Berkeley" - Tabled indefinitely, $0
- Agenda shows: "Helix @ Berkeley" - Approved, $100

The **Reconciliation Processor** resolves these conflicts by:
1. Merging both datasets on `Org Name`
2. Using Agenda values for `Amount` and `Committee Status` when both exist
3. Including organizations from both sources (union operation)

## How It Works

### Input Requirements

The reconciliation process requires two CSV files in the `files/output/` directory:

1. **FR Output**: File containing "Cleaned" in the name (e.g., `FR 24_25 S2 Cleaned.csv`)
   - Columns: `Org Name`, `Request Type`, `Amount`, `Committee Status`, etc.

2. **Agenda Output**: File containing "Agenda" in the name (e.g., `2024-11-04 Agenda.csv`)
   - Columns: `Org Name`, `Request Type`, `Amount`, `Committee Status`, `Date`

### Reconciliation Logic

For each organization:

| Scenario | Action |
|----------|--------|
| Org in both FR and Agenda | Use Agenda's `Amount` and `Committee Status` |
| Org only in FR | Keep FR's original values |
| Org only in Agenda | Include with Agenda's values |

### Output

Creates a reconciled CSV file in `files/finaloutput/` with naming format: `{FR_filename} Finalized.csv`

Example: If input is `FR 24_25 S2 Cleaned.csv`, output will be `FR 24_25 S2 Finalized.csv`

Output columns (in this exact order):
1. `Org Name` - Organization name
2. `Request Type` - Type of funding request (Contingency, Finance Rule, etc.)
3. `Amount Requested` - Original requested amount from FR (if available)
4. `Amount` - Allocated amount (from Agenda if available, else FR). **Set to 0 if not approved**
5. `Committee Status` - Decision status (from Agenda if available, else FR)
6. `Source` - Indicates data source:
   - `"Both (Agenda Priority)"` - Org in both datasets, Agenda values used
   - `"FR Only"` - Org only in FR dataset
   - `"Agenda Only"` - Org only in Agenda dataset
7. `Org Type (year)` - From FR (if available)
8. `Date` - Date from Agenda (if available)

**Row Sorting**: Rows are sorted by Request Type (Contingency → Finance Rule → Space Reservation → Sponsorship → Others), then alphabetically by Org Name within each type.

## Usage

### Step 1: Process FR and Agenda Files

First, run the FR and Agenda processors separately:

```bash
# Process FR file
python3 execute.py --input files/input --output files/output --process fr

# Process Agenda file
python3 execute.py --input files/input --output files/output --process contingency
```

Or use the existing `run.py` scripts.

### Step 2: Run Reconciliation

Once both outputs exist in `files/output/`, run:

```bash
python3 run_reconcile.py
```

Or manually:

```bash
python3 execute.py --input files/output --output files/output --process reconcile
```

### Expected Console Output

```
[RECONCILE PULL] Using FR file: FR 24_25 S2 Cleaned.csv
[RECONCILE PULL] Using Agenda file: 2024-11-04 Agenda.csv
[RECONCILE] Summary:
  - Organizations in both FR and Agenda: 15 (Agenda data used)
  - Organizations only in FR: 3
  - Organizations only in Agenda: 2
  - Total organizations: 20
```

## Example

### Before Reconciliation

**FR Output:**
| Org Name | Request Type | Amount | Committee Status |
|----------|-------------|--------|------------------|
| Helix @ Berkeley | Contingency | NaN | Tabled indefinitely |
| Club A | Contingency | 500 | Approved |

**Agenda Output:**
| Org Name | Request Type | Amount | Committee Status |
|----------|-------------|--------|------------------|
| Helix @ Berkeley | Contingency | 100 | Approved |
| Club B | Contingency | 200 | Approved |

### After Reconciliation

**Reconciled Output:**
| Org Name | Request Type | Amount | Committee Status | Source |
|----------|-------------|--------|------------------|--------|
| Helix @ Berkeley | Contingency | 100 | Approved | Both (Agenda Priority) |
| Club A | Contingency | 500 | Approved | FR Only |
| Club B | Contingency | 200 | Approved | Agenda Only |

## Files Modified/Created

### New Files
- `ASFINT/Transform/Reconciliation_Processor.py` - Core reconciliation logic
- `run_reconcile.py` - Convenience script to run reconciliation
- `RECONCILIATION_README.md` - This documentation

### Modified Files
- `ASFINT/Config/Config.py` - Added RECONCILE process type
- `ASFINT/Pull/pullers.py` - Added `pull_reconcile()` function
- `ASFINT/Transform/Processor.py` - Added `reconcile()` method
- `ASFINT/Transform/__init__.py` - Exported `Reconcile_FR_Agenda`

## Troubleshooting

### Error: "No FR file found"
**Cause**: No file with "Cleaned" in filename exists in `files/output/`

**Solution**: Run FR processor first:
```bash
python3 execute.py --input files/input --output files/output --process fr
```

### Error: "No Agenda file found"
**Cause**: No file with "Agenda" in filename exists in `files/output/`

**Solution**: Run Agenda processor first:
```bash
python3 execute.py --input files/input --output files/output --process contingency
```

### Error: "Org Name column missing"
**Cause**: Input files don't have required `Org Name` column

**Solution**: Verify input CSV files have correct column structure from FR/Agenda processors

## Technical Details

### Data Cleaning
- Organization names are stripped of whitespace before matching
- Uses pandas `combine_first()` for prioritization (Agenda > FR)
- Outer join ensures no organizations are lost

### Source Tracking
The `Source` column helps identify which dataset contributed each row:
- Use this for auditing and quality assurance
- Filter by source to see dataset-specific entries

### Performance
- Efficient for typical committee datasets (10-100 organizations)
- Uses pandas merge operations (O(n log n) complexity)

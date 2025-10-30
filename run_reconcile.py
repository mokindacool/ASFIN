"""
run_reconcile.py

Script to run the reconciliation process after both FR and Agenda processors have completed.

NEW: Now supports BATCH mode using automatic date-based matching!

Usage:
    # Batch mode (recommended) - automatically matches all FR+Agenda pairs by date:
    python3 run_reconcile.py --batch

    # Single mode (legacy) - uses execute.py to process one pair:
    python3 run_reconcile.py

Prerequisites:
    - FR Cleaned output file(s) with Date column must exist in files/output/
    - Agenda output file(s) with Date column must exist in files/output/

Output:
    - Reconciled CSV file(s) will be created in files/finaloutput/
      with names like "FR 24_25 S1 Finalized.csv"
"""

import subprocess
import argparse
from pathlib import Path


def run_batch_reconcile(input_path: Path, output_path: Path):
    """
    Run batch reconciliation using automatic date-based matching.
    This is the new recommended approach.
    """
    from ASFINT.Transform.Reconciliation_Processor import Reconcile_FR_Agenda_Batch

    print("=" * 70)
    print("BATCH RECONCILIATION (Date-Based Matching)")
    print("=" * 70)
    print(f"Input folder:  {input_path}")
    print(f"Output folder: {output_path}")
    print("=" * 70)
    print()

    try:
        # Run batch reconciliation
        results = Reconcile_FR_Agenda_Batch(str(input_path))

        # Save all results
        print("\n" + "=" * 70)
        print("SAVING RECONCILED FILES")
        print("=" * 70)

        for name, df in results.items():
            output_file = output_path / f'{name}.csv'
            df.to_csv(output_file, index=False)
            print(f"✓ Saved: {output_file}")
            print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")

        print("\n" + "=" * 70)
        print("✅ BATCH RECONCILIATION COMPLETE")
        print("=" * 70)
        print(f"Total files reconciled: {len(results)}")
        print(f"Check {output_path}/ for all reconciled files")

    except Exception as e:
        print(f"\n❌ Batch reconciliation failed: {e}")
        import traceback
        traceback.print_exc()


def run_single_reconcile(input_path: Path, output_path: Path):
    """
    Run single reconciliation using execute.py (legacy approach).
    This processes only the most recent FR+Agenda pair.
    """
    process_type = "reconcile"

    cmd = [
        "python3", "execute.py",
        "--input", str(input_path),
        "--output", str(output_path),
        "--process", process_type
    ]

    print("=" * 70)
    print("SINGLE RECONCILIATION (Legacy Mode)")
    print("=" * 70)
    print(f"Input folder:  {input_path}")
    print(f"Output folder: {output_path}")
    print(f"Process type:  {process_type}")
    print("=" * 70)
    print("\n⚠ NOTE: This mode only processes the most recent FR+Agenda pair.")
    print("   Use --batch mode to reconcile all pairs automatically.\n")

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"❌ Reconciliation failed with code {result.returncode}")
        print("\nSTDOUT:")
        print(result.stdout)
        print("\nSTDERR:")
        print(result.stderr)
    else:
        print("✅ Reconciliation completed successfully!")
        print(result.stdout)
        print()
        print("=" * 70)
        print("Check files/finaloutput/ for the reconciled CSV file")
        print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reconcile FR and Agenda outputs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Batch mode (recommended) - process all FR+Agenda pairs:
  python3 run_reconcile.py --batch

  # Single mode - process only most recent pair:
  python3 run_reconcile.py
        """
    )
    parser.add_argument(
        '--batch',
        action='store_true',
        help='Use batch mode to reconcile all FR+Agenda pairs by date (recommended)'
    )

    args = parser.parse_args()

    # Base directory = current working directory
    base_dir = Path.cwd()

    # Input path = output folder (where FR and Agenda outputs are)
    input_path = base_dir / "files" / "output"
    output_path = base_dir / "files" / "finaloutput"

    # Make sure folders exist
    input_path.mkdir(parents=True, exist_ok=True)
    output_path.mkdir(parents=True, exist_ok=True)

    # Run appropriate mode
    if args.batch:
        run_batch_reconcile(input_path, output_path)
    else:
        run_single_reconcile(input_path, output_path)

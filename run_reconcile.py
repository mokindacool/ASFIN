"""
run_reconcile.py

Script to run the reconciliation process after both FR and Agenda processors have completed.

Usage:
    python3 run_reconcile.py

Prerequisites:
    - FR Cleaned output file (e.g., "FR 24_25 S2 Cleaned.csv") must exist in files/output/
    - Agenda output file (e.g., "2024-11-04 Agenda.csv") must exist in files/output/

Output:
    - Reconciled CSV file will be created in files/finaloutput/ with name like "FR 24_25 S2 Finalized.csv"
"""

import subprocess
from pathlib import Path

if __name__ == "__main__":
    # Base directory = current working directory
    base_dir = Path.cwd()

    # Input path = output folder (where FR and Agenda outputs are)
    input_path = base_dir / "files" / "output"
    output_path = base_dir / "files" / "finaloutput"
    process_type = "reconcile"

    # Make sure folders exist
    input_path.mkdir(parents=True, exist_ok=True)
    output_path.mkdir(parents=True, exist_ok=True)

    # Build the command
    cmd = [
        "python3", "execute.py",
        "--input", str(input_path),
        "--output", str(output_path),
        "--process", process_type
    ]

    print("=" * 60)
    print("RECONCILIATION PROCESS")
    print("=" * 60)
    print(f"Input folder:  {input_path}")
    print(f"Output folder: {output_path}")
    print(f"Process type:  {process_type}")
    print("=" * 60)
    print()

    # Run the command
    result = subprocess.run(cmd, capture_output=True, text=True)

    # Check return code
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
        print("=" * 60)
        print("Check files/finaloutput/ for the reconciled CSV file")
        print("=" * 60)

import subprocess
from pathlib import Path

if __name__ == "__main__":
    # Base directory = current working directory
    base_dir = Path.cwd()

    # Input/output paths relative to CWD
    input_path = base_dir / "files" / "input"
    output_path = base_dir / "files" / "output"
    process_type = "contingency" # change to contingency

    # Make sure folders exist
    input_path.mkdir(parents=True, exist_ok=True)
    output_path.mkdir(parents=True, exist_ok=True)

    # Build the command
    cmd = [
        "python", "execute.py",
        "--input", str(input_path),
        "--output", str(output_path),
        "--process", process_type
    ]

    # Run the command
    result = subprocess.run(cmd, capture_output=True, text=True)

    # Check return code
    if result.returncode != 0:
        print(f"run.py exited with code {result.returncode}")
    else:
        print("run.py completed successfully")

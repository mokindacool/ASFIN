import pandas as pd
from pathlib import Path

def pull_csv(filepath, process_type):
    """
    Pull raw CSVs.
    - If `filepath` is a file: read it directly.
    - If `filepath` is a directory: scan for all CSV files inside.
    Returns {filename: DataFrame} (or (df, text) for FR).
    """
    filepath = Path(filepath)

    files = {}
    if filepath.is_file():
        df = pd.read_csv(filepath)
        files[filepath.stem] = df if process_type != "FR" else (df, filepath.read_text(errors="ignore"))
    elif filepath.is_dir():
        for f in filepath.glob("*.csv"):
            df = pd.read_csv(f)
            if process_type == "FR":
                # FR expects (df, text)
                files[f.stem] = (df, f.read_text(errors="ignore"))
            else:
                files[f.stem] = df
    else:
        raise FileNotFoundError(f"Input path does not exist: {filepath}")

    return files
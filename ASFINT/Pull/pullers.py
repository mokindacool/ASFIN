import pandas as pd
from pathlib import Path


def pull_csv(path, process_type):
    """
    Load .csv inputs.
    - If `path` is a directory: read all *.csv files into {stem: DataFrame}.
    - If `path` is a file: read that file only.
    FR note: if FR expects (df, text), adapt here (e.g., pair with a .txt of same stem).
    """
    p = Path(path)

    if not p.exists():
        raise FileNotFoundError(f"Input path does not exist: {p}")

    out = {}
    if p.is_dir():
        for f in sorted(p.glob("*.csv")):
            df = pd.read_csv(f)
            out[f.stem] = df
    else:
        if p.suffix.lower() != ".csv":
            raise ValueError(f"Expected a .csv file, got: {p.name}")
        df = pd.read_csv(p)
        out[p.stem] = df

    return out

def pull_txt(path, process_type):
    """
    Load .txt inputs.
    - If `path` is a directory: read all *.txt files into {stem: text}.
    - If `path` is a file: read that file only.
    """
    p = Path(path)

    if not p.exists():
        raise FileNotFoundError(f"Input path does not exist: {p}")

    out = {}
    if p.is_dir():
        for f in sorted(p.glob("*.txt")):
            text = f.read_text(encoding="utf-8", errors="ignore")
            out[f.stem] = text
    else:
        if p.suffix.lower() != ".txt":
            raise ValueError(f"Expected a .txt file, got: {p.name}")
        text = p.read_text(encoding="utf-8", errors="ignore")
        out[p.stem] = text

    return out

def pull_fr(path, process_type):
    """
    Load FR inputs as (DataFrame, text) tuples.
    - Pairs .csv files with matching .txt files by stem name
    - If no matching .txt exists, uses empty string as text
    - Returns {stem: (DataFrame, text)}
    """
    p = Path(path)

    if not p.exists():
        raise FileNotFoundError(f"Input path does not exist: {p}")

    out = {}
    if p.is_dir():
        # Find all CSV files
        for csv_file in sorted(p.glob("*.csv")):
            df = pd.read_csv(csv_file)
            # Look for matching TXT file
            txt_file = csv_file.with_suffix(".txt")
            if txt_file.exists():
                text = txt_file.read_text(encoding="utf-8", errors="ignore")
            else:
                text = ""  # Empty string if no matching txt
            out[csv_file.stem] = (df, text)
    else:
        if p.suffix.lower() != ".csv":
            raise ValueError(f"Expected a .csv file, got: {p.name}")
        df = pd.read_csv(p)
        # Look for matching TXT file
        txt_file = p.with_suffix(".txt")
        if txt_file.exists():
            text = txt_file.read_text(encoding="utf-8", errors="ignore")
        else:
            text = ""  # Empty string if no matching txt
        out[p.stem] = (df, text)

    return out
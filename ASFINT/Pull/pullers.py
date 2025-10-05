import pandas as pd
from pathlib import Path

def pull_csv(filepath, process_type):
    """
    Pull raw CSVs.
    - If `filepath` is a file: read it directly.
    - If `filepath` is a directory: scan for all CSV files inside.
    Returns {filename: DataFrame} (or (df, text) for FR).
    """
    process_type = process_type.strip().upper()
    if process_type == 'FR':
        with open(Path(filepath), 'rb') as f:
            text = f.read().decode('utf-8')
        return (pd.read_csv(str(filepath)), text)
    return pd.read_csv(str(filepath))

def pull_txt(filepath: str|Path, process_type: str):
    """
    Reads a delimited text file from a filepath into a pandas DataFrame.
    Assumes the delimiter is a tab.

    Only used for Agenda Processor. 
    """
    # process_type = process_type.strip().upper()
    with open(Path(filepath), 'rb') as f:
        text = f.read().decode('utf-8')
    return text

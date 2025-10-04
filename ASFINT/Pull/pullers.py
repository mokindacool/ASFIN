import pandas as pd
from pathlib import Path

def pull_csv(filepath: str|Path, process_type: str):
    """
    Takes in a filepath and converts it into a csv.
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
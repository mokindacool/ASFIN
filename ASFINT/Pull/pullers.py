from .pullers import *
import pandas as pd
from pathlib import Path

def pull_csv(filepath: str|Path, process_type: str):
    """
    Takes in a filepath and converts it into a csv.
    """
    if process_type == 'FR':
        with open(Path(filepath), 'rb') as f:
            text = f.read().decode('utf-8')
        return (pd.read_csv(str(filepath)), text)
    return pd.read_csv(str(filepath))
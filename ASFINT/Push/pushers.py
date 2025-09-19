import pandas as pd
from pathlib import Path

def push_csv(df: pd.DataFrame, filename: str, filepath: str|Path):
    """
    Takes in a filepath and converts it into a csv.
    """
    filepath = Path(filepath)
    filepath.mkdir(parents=True, exist_ok=True)
    outpath = filepath / f"{filename}.csv"
    df.to_csv(outpath, index=False)
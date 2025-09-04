import pandas as pd
from pathlib import Path

def push_csv(df: pd.DataFrame, filename: str, filepath: str|Path):
    """
    Takes in a filepath and converts it into a csv.
    """
    df.to_csv(Path(filepath)/filename, index=False)
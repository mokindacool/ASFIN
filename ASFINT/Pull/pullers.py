from .pullers import *
import pandas as pd
from pathlib import Path

def pull_csv(filepath: str|Path):
    """
    Takes in a filepath and converts it into a csv.
    """
    return pd.read_csv(str(filepath))
from collections.abc import Iterable
import os
from pathlib import Path
import pandas as pd

from ASFINT.Config.config import get_pFuncs

# ---------
# Pull, Push and Process functions
# ---------

def pull(path: str, process_type: str) -> dict[int, pd.DataFrame] | dict[int, pd.DataFrame]:
    """
    Pulls files from a local folder or single file and returns
    { filename : DataFrame } for ASUCProcessor
    """
    target_path = Path(path)

    if not target_path.exists():
        print(f"Error: Path '{target_path}' does not exist.")
        return None

    puller = get_pFuncs(process_type=process_type, func='pull')

    # Case 1: Path is a FILE
    if target_path.is_file():
        print(f"Path is a file. Reading '{target_path.name}'...")
        try:
            df = puller(target_path)
            return {target_path.name: df}
        except Exception as e:
            print(f"Could not read or process file '{target_path.name}' with puller '{puller.__name__}': {e}")
            return None

    # Case 2: Path is a DIRECTORY
    elif target_path.is_dir():
        print(f"Path is a directory. Searching for files in '{target_path.name}'...\n")
        files = {}
        for item in target_path.iterdir():
            if item.is_file():
                try:
                    df = puller(item)
                    files[item.name] = df
                except Exception as e:
                    print(f"Could not read or process file '{item.name}' with puller '{puller.__name__}': {e}")

        if not files:
            print(f"No files found in the directory '{target_path.name}'.")
            return None
        return files

    # Case 3: Invalid path
    else:
        print(f"Error: Path '{target_path}' is not a valid file or directory.")
        return None

def push(dfs: dict[str:pd.DataFrame], path: str, process_type: str) -> None:
    """
    Saves a list of transformed files onto some local folder
    """
    if isinstance(dfs, pd.DataFrame): dfs = [dfs]
    target_path = Path(path)
    if ~target_path.is_dir(): raise ValueError(f"Inputted path must be to a existing directory, current path is invalid: {path}")

    pusher = get_pFuncs(process_type=process_type, func='push') 
    for name, dataframe in dfs.items():
        pusher(dataframe, name, target_path)


def process(files: dict[str:pd.DataFrame], process_type: str) -> dict[str:pd.DataFrame]:
    """
    Calls ASUCProcessor to process a list of files
    """
    if isinstance(files, pd.DataFrame): files = [files]

    processor = get_pFuncs(process_type=process_type, func='process')
    for f in files: 
        pass


# --------
# run function that wraps everything together
# --------

def run(pull_path: str, push_path: str, process_type: str):
    pass

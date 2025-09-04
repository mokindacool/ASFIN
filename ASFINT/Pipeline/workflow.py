from collections.abc import Iterable
import os
from pathlib import Path
import pandas as pd
import argparse

from ASFINT.Config.config import get_pFuncs

# ---------
# Pull, Push and Process functions
# ---------

def pull(path: str, process_type: str) -> dict[str, pd.DataFrame]:
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

def push(dfs: dict[str, pd.DataFrame], path: str, process_type: str) -> None:
    """
    Saves a list of transformed files onto some local folder
    """
    target_path = Path(path)
    if not target_path.is_dir(): raise ValueError(f"Inputted path must be to a existing directory, current path is invalid: {path}")

    pusher = get_pFuncs(process_type=process_type, func='push') 
    for name, dataframe in dfs.items():
        pusher(dataframe, name, target_path)


def process(files: dict[str, pd.DataFrame], process_type: str) -> dict[str, pd.DataFrame]:
    """
    Calls ASUCProcessor to process a list of files
    """

    processor = get_pFuncs(process_type=process_type, func='process')
    df_lst, new_names = processor(dfs=files.values(), names=files.keys())
    return dict(zip(new_names, df_lst))


# --------
# run function that wraps everything together
# --------
def run(pull_path: str, push_path: str, process_type: str):
    raw_dict = pull(path=pull_path, process_type=process_type)
    clean_dict = process(files=raw_dict, process_type=process_type)
    push(dfs=clean_dict, path=push_path, process_type=process_type)

def main(manual=None, args=None):
    if manual is not None:
        # Programmatic call, no CLI parsing
        run(
            pull_path=manual.get('pullPath'), 
            push_path=manual.get('pushPath'), 
            process_type=manual.get('processType'),
        )
        return
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--processType", type=str, required=True)
    parser.add_argument("--pullPath", type=str, required=True)
    parser.add_argument("--pushPath", type=str, required=True)

    parsed_args = parser.parse_args(args)

    run(
        pull_path=parsed_args.pullPath, 
        push_path=parsed_args.pushPath, 
        process_type=parsed_args.processType,
        )

if __name__ == "__main__":
    main()

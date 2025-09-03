from collections.abc import Iterable
import os
from pathlib import Path

from ASFINT.Config.config import get_pushpull

# ---------
# Pull, Push and Process functions
# ---------

def pull(path: str, process_type: str):
    """
    Pulls files from a local folder and renders them into CSVs/strings for ASUCProcessor
    id: folder or file id
    """
    target_path = Path(path)

    # Check if the path exists
    if not target_path.exists():
        print(f"Error: Path '{target_path}' does not exist.")
        return

    # Case 1: The path is a FILE
    if target_path.is_file():
        print(f"Path is a file. Reading '{target_path.name}'...")
        try:
            puller = get_pushpull(process_type=process_type, func='pull')
            file = puller(target_path) 
            return file
        except Exception as e:
            print(f"Could not read or process file '{target_path.name}' with puller '{puller.__name__}': {e}")

    # Case 2: The path is a DIRECTORY
    elif target_path.is_dir():
        print(f"Path is a directory. Searching for files in '{target_path.name}'...\n")
        files = []
        for item in target_path.iterdir():
            if item.is_file():
                try:
                    puller = get_pushpull(process_type=process_type, func='pull')
                    f = puller(target_path)
                    files.append(f)
                except Exception as e:
                    print(f"Could not read or process file '{item.name}' with puller '{puller.__name__}': {e}")
        
        if len(files) == 0:
            print(f"No files found in the directory '{target_path.name}'.")
            return None
        else:
            return files

    # Case 3: The path is neither a file nor a directory
    else:
        print(f"Error: Path '{target_path}' is not a valid file or directory.")

def push(files: Iterable, path: str, process_type: str):
    """
    Saves a list of transformed files onto some local folder
    """
    pass

def process(files: Iterable, process_type: str):
    """
    Calls ASUCProcessor to process a list of files
    """
    pass

# --------
# run function that wraps everything together
# --------

def run(pull_path: str, push_path: str, process_type: str):
    pass

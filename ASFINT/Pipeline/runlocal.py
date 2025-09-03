from collections.abc import Iterable
# ---------
# Pull, Push and Process functions
# ---------

def pull(path: str, process_type: str):
    """
    Pulls files from a local folder and renders them into CSVs/strings for ASUCProcessor
    id: folder or file id
    """
    pass

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

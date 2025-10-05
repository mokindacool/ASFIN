"""
workflow.py
ASFINT Pipeline orchestration:
  - pull → process → push

Handles high-level ETL for ABSA, OASIS, FR, Agenda.
"""

import os
from ASFINT.Config.Config import get_pFuncs, get_naming  # keep as-is
from ASFINT.Utility.Utils import ensure_folder


def pull(path: str, process_type: str):
    """
    Load raw files into memory as {filename: DataFrame} (or (DataFrame, text) for FR).
    """
    pull_func = get_pFuncs(process_type, "pull")
    return pull_func(path, process_type)


def process(files: dict, process_type: str, reporting: bool = False):
    """
    Dispatch to processor with a local import to avoid circular imports.
    """
    # ✅ Import the actual module by its filename (lowercase processor.py)
    try:
        from ..Transform import processor as Processor  # relative package import
    except Exception:
        # Fallback to absolute import if relative fails
        import importlib
        Processor = importlib.import_module("ASFINT.Transform.processor")

    results = {}
    try:
        if reporting:
            print(f"[INFO] Processing {len(files)} files for process_type={process_type}")
        results = Processor.process(files, process_type)
    except Exception as e:
        file_list = list(files.keys())
        print(f"[ERROR] Processing failed for process_type={process_type}. Files: {file_list}. Error: {e}")
        # ... keep your errored-dump code here ...
        return {}
    return results

def push(dfs: dict, path: str, process_type: str):
    """
    Write cleaned DataFrames to disk using configured naming.
    """
    push_func = get_pFuncs(process_type, "push")
    for fname, df in dfs.items():
        try:
            push_func(df, fname, path)
        except Exception as e:
            print(f"[ERROR] Failed to push '{fname}' for {process_type}: {e}")


def run(pull_path: str, push_path: str, process_type: str, reporting: bool = False):
    """
    Full ETL orchestration: pull → process → push.
    Normalize process_type to uppercase for config lookups.
    """
    # normalize process type so 'fr' works
    pt = (process_type or "").upper()

    ensure_folder(pull_path)
    ensure_folder(push_path)

    if reporting:
        print(f"[INFO] Starting pipeline for process_type={process_type}")

    files = pull(pull_path, pt)
    cleaned = process(files, pt, reporting=reporting)
    push(cleaned, push_path, pt)

def process(files: dict[str, pd.DataFrame], process_type: str, reporting=False) -> dict[str, pd.DataFrame]:
    """
    Calls ASUCProcessor to process a list of files
    """

    processor = get_pFuncs(process_type=process_type, func='process')
    df_lst, new_names = processor(files.values(), files.keys(), reporting) # processor will automatically query for keys and values to get names and dataframes
    return dict(zip(new_names, df_lst))


# --------
# run function that wraps everything together
# --------
def run(pull_path: str, push_path: str, process_type: str, reporting=False):
    print(f"Running pipeline, process type: {process_type}")
    raw_dict = pull(path=pull_path, process_type=process_type, reporting=reporting)
    clean_dict = process(files=raw_dict, process_type=process_type, reporting=reporting)
    push(dfs=clean_dict, path=push_path, process_type=process_type, reporting=reporting)

"""
Processor.py
Dispatch & orchestration for ASFIN/ASFINT processors.

- Defines ASUCProcessor (class-based API).
- Provides top-level process(files, process_type) function
  for compatibility with existing Pipeline/workflow.py.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Tuple, Dict, Any

import pandas as pd

from ASFINT.Utility.Cleaning import is_type
from ASFINT.Utility.Logger_Utils import get_logger

# Use concrete module imports (more reliable for editors/runners)
from ASFINT.Transform.ABSA_Processor import ABSA_Processor
from ASFINT.Transform.Agenda_Processor import Agenda_Processor
from ASFINT.Transform.OASIS_Processor import OASIS_Abridged
from ASFINT.Transform.FR_Processor import FR_ProcessorV2
from ASFINT.Transform.Reconciliation_Processor import Reconcile_FR_Agenda


class ASUCProcessor:
    """
    Wrapper for per-dataset processors.

    Typical flow (external):
        proc = ASUCProcessor("FR")
        dfs_out, names_out = proc.fr(dfs_in, names_in, reporting=True)
    """

    def __init__(self, process_type: str):
        self.type = process_type.upper()
        self.logger = get_logger(self.type)
        self.processors = {
            "ABSA": self.absa,
            "OASIS": self.oasis,
            "FR": self.fr,
            "CONTINGENCY": self.contingency,
            "RECONCILE": self.reconcile,
        }

    # --------------------------
    # Configuration (static)
    # --------------------------
    @staticmethod
    def get_process_configs() -> Dict[str, Dict[str, Any]]:
        if hasattr(ASUCProcessor, "process_configs"):
            return ASUCProcessor.process_configs

        ASUCProcessor.process_configs = {
            "ABSA": {
                "Clean Tag": "GF",
                "Raw Tag": "RF",
                "Raw Name Dependency": "ABSA",
                "Clean File Name": "ABSA",
                "Processing Function": ABSA_Processor,
            },
            "OASIS": {
                "Clean Tag": "GF",
                "Raw Tag": None,
                "Raw Name Dependency": None,
                "Clean File Name": "OASIS",
                "Processing Function": OASIS_Abridged,
            },
            "FR": {
                "Clean Tag": "GF",
                "Raw Tag": None,
                "Raw Name Dependency": None,
                "Clean File Name": "FR",
                "Processing Function": FR_ProcessorV2,
            },
            "CONTINGENCY": {
                "Clean Tag": "GF",
                "Raw Tag": None,
                "Raw Name Dependency": None,
                "Clean File Name": "Agenda",
                "Processing Function": Agenda_Processor,
            },
            "RECONCILE": {
                "Clean Tag": "GF",
                "Raw Tag": None,
                "Raw Name Dependency": None,
                "Clean File Name": "Reconciled",
                "Processing Function": Reconcile_FR_Agenda,
            },
        }
        return ASUCProcessor.process_configs

    def get_processing_func(self):
        cfg = ASUCProcessor.get_process_configs()
        return cfg.get(self.get_type()).get("Processing Function")

    def get_type(self) -> str:
        return self.type

    def get_file_naming(self, tag_type: str = "Clean") -> str:
        cfg = ASUCProcessor.get_process_configs()
        if tag_type != "Clean":
            raise ValueError(f"Unknown tag type {tag_type}")
        return cfg.get(self.get_type()).get("Clean File Name")

    def get_name_dependency(self) -> str | None:
        cfg = ASUCProcessor.get_process_configs()
        return cfg.get(self.get_type()).get("Raw Name Dependency")

    def _log(self, msg: str, reporting: bool) -> None:
        if reporting:
            print(msg)
        else:
            self.logger.info(msg)

    def name_clean(self, names: Iterable[str], subst_name: str | None = None, reporting: bool = False) -> List[str]:
        out: List[str] = []
        for raw in names:
            assert isinstance(raw, str), "expected name to be a string"
            new = re.sub(r"\([\d]+\)", "", raw)
            new = new.replace(".gsheet", "").replace(".xlsx", "").replace(".csv", "").strip()
            dep = self.get_name_dependency()
            if subst_name and dep:
                new = new.replace(dep, subst_name)
            self._log(f"Cleaned name: '{raw}' -> '{new}'", reporting)
            out.append(new)
        return out

    # --------------------------
    # ABSA
    # --------------------------
    def absa(self, dfs: Iterable[pd.DataFrame], names: Iterable[str], reporting: bool = False) -> Tuple[List[pd.DataFrame], List[str]]:
        dfs = list(dfs)
        names = list(names)
        names = self.name_clean(names=names, subst_name="ABSA", reporting=reporting)

        out_frames: List[pd.DataFrame] = []
        out_names: List[str] = []

        for df, name in zip(dfs, names):
            assert isinstance(df, pd.DataFrame), "expected a pandas DataFrame"
            try:
                fn = self.get_processing_func()
                processed = fn(df)
                out_frames.append(processed)
                out_names.append(self.get_file_naming())
                self._log(f"[ABSA] Processed '{name}' via {fn.__name__}", reporting)
            except Exception as e:
                self._log(f"Processing failed for {name}, processing function: {self.get_processing_func().__name__}) : {str(e)}", reporting)
                raise e
        return rv, names
    
    def contingency(self, txt_lst: Iterable[str], names: Iterable[str], reporting = False) -> list[pd.DataFrame]:
        """
        Function that takes in a dictionary of txt files and names then outputs a dictionary of processed txt files with updated names. 
        Date is appended to updated file names under formatting: %m/%d/%Y.
        """
        if isinstance(txt_lst, str): txt_lst = [txt_lst]
        if isinstance(names, str): names = [names]

        txt_lst = list(txt_lst)
        names = list(names)

        rv = []
        for i in range(len(txt_lst)): 
            txt = txt_lst[i]
            name = names[i]

            # Name Validation
            mismatch = False
            if 'ficomm' not in name.lower() and 'finance committee' not in name.lower():
                self._log(f"Name mismatch: {name}", reporting)
                mismatch = True

            # Date Formatting Output
            t = self.get_type()
            date_format = self.get_config(process=t, key='Date Format', substitute="%m-%d-%Y")

            # Processing 
            try:
                processing_function = self.get_processing_func()
                output, date = processing_function(txt, date_format=date_format, debug=False)
                rv.append(output)
                self._log(f"Successfully processed {name}", reporting)
            except Exception as e:
                self._log(f"Processing failed for {name}: {str(e)}", reporting)
                raise e
            
            # Renaming
            if mismatch:
                names[i] = f"{self.get_file_naming(tag_type = 'Clean')}-{fiscal_year}-{date_formatted}-MISMATCH"
            else:
                date_formatted = pd.Timestamp(date).strftime(date_format)
                fiscal_year = f"FY{str(pd.Timestamp(date).year)[-2:]}" # formatting to FY24, FY25, etc
                validated_name = f"{self.get_file_naming(tag_type = 'Clean')}-{fiscal_year}-{date_formatted}-{self.get_tagging(tag_type = 'Clean')}" # Contingency draws from ficomm files formatted "Ficomm-date-RF"
                names[i] = validated_name
        return rv, names
    
    def oasis(self, dfs: Iterable[pd.DataFrame], names: Iterable[str], reporting = False) -> list[pd.DataFrame]:
        if isinstance(dfs, pd.DataFrame): dfs = [dfs]
        if isinstance(names, str): names = [names]

    # --------------------------
    # OASIS
    # --------------------------
    def oasis(self, dfs: Iterable[pd.DataFrame], names: Iterable[str], reporting: bool = False, year: str = "2024") -> Tuple[List[pd.DataFrame], List[str]]:
        dfs = list(dfs)
        names = list(names)
        names = self.name_clean(names=names, subst_name="OASIS", reporting=reporting)

        out_frames: List[pd.DataFrame] = []
        out_names: List[str] = []

        for df, name in zip(dfs, names):
            assert isinstance(df, pd.DataFrame), "expected a pandas DataFrame"
            try:
                fn = self.get_processing_func()
                processed = fn(df, year=year)
                out_frames.append(processed)
                out_names.append(self.get_file_naming())
                self._log(f"[OASIS] Processed '{name}' via {fn.__name__}", reporting)
            except Exception as e:
                self._log(f"[OASIS] Failed '{name}': {e}", reporting)
                raise
        return out_frames, out_names

    # --------------------------
    # FR
    # --------------------------
    def fr(self, dfs: Iterable[Tuple[pd.DataFrame, str]], names: Iterable[str], reporting: bool = False) -> Tuple[List[pd.DataFrame], List[str]]:
        dfs = list(dfs)
        names = list(names)
        original_names = list(names)  # Store original names before cleaning
        names = self.name_clean(names=names, subst_name="FR", reporting=reporting)

        out_frames: List[pd.DataFrame] = []
        out_names: List[str] = []

        for idx, pair in enumerate(dfs):
            if not isinstance(pair, (tuple, list)) or len(pair) != 2:
                raise ValueError(f"[FR] Expected (df, txt) tuple at index {idx}")
            df, txt = pair
            assert isinstance(df, pd.DataFrame), "expected a pandas DataFrame"

            try:
                fn = self.get_processing_func()  # FR_ProcessorV2
                # Pass the original filename so output can be "{original_name} Cleaned"
                orig_name = original_names[idx] if idx < len(original_names) else None
                produced: Dict[str, pd.DataFrame] = fn(df, txt, date_format="%Y-%m-%d", original_filename=orig_name)
                for out_name, out_df in produced.items():
                    out_frames.append(out_df)
                    out_names.append(out_name)
                    self._log(f"[FR] Produced '{out_name}' via {fn.__name__}", reporting)
            except Exception as e:
                src_name = names[idx] if idx < len(names) else f"index-{idx}"
                self._log(f"[FR] Failed '{src_name}': {e}", reporting)
                raise
        return out_frames, out_names

    # --------------------------
    # CONTINGENCY
    # --------------------------
    def contingency(self, texts: Iterable[str], names: Iterable[str], reporting: bool = False) -> Tuple[List[pd.DataFrame], List[str]]:
        texts = list(texts)
        names = list(names)
        original_names = list(names)  # Store original names before cleaning
        names = self.name_clean(names=names, subst_name="Agenda", reporting=reporting)

        out_frames: List[pd.DataFrame] = []
        out_names: List[str] = []

        for idx, (text, name) in enumerate(zip(texts, names)):
            assert isinstance(text, str), "expected a text string"
            try:
                fn = self.get_processing_func()
                processed, date = fn(text)
                out_frames.append(processed)

                # Extract date from original filename (e.g., "2024-11-04 Finance Committee Agenda & Minutes.txt")
                orig_name = original_names[idx] if idx < len(original_names) else name
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', orig_name)
                if date_match:
                    file_date = date_match.group(1)
                    output_name = f"{file_date} {self.get_file_naming()}"
                else:
                    output_name = self.get_file_naming()

                out_names.append(output_name)
                self._log(f"[CONTINGENCY] Processed '{name}' via {fn.__name__}", reporting)
            except Exception as e:
                self._log(f"[CONTINGENCY] Failed '{name}': {e}", reporting)
                raise
        return out_frames, out_names

    # --------------------------
    # RECONCILE
    # --------------------------
    def reconcile(self, tuples: Iterable[Tuple[pd.DataFrame, pd.DataFrame, str]], names: Iterable[str], reporting: bool = False) -> Tuple[List[pd.DataFrame], List[str]]:
        """
        Reconcile FR and Agenda outputs.

        Args:
            tuples: Iterable of (fr_df, agenda_df, fr_filename) tuples
            names: Iterable of file names (not used for reconcile, but required by interface)
            reporting: Whether to print progress logs

        Returns:
            Tuple of (list of reconciled DataFrames, list of output names)
        """
        tuples = list(tuples)
        names = list(names)

        out_frames: List[pd.DataFrame] = []
        out_names: List[str] = []

        for idx, triplet in enumerate(tuples):
            if not isinstance(triplet, (tuple, list)) or len(triplet) != 3:
                raise ValueError(f"[RECONCILE] Expected (fr_df, agenda_df, fr_filename) tuple at index {idx}")

            fr_df, agenda_df, fr_filename = triplet

            if not isinstance(fr_df, pd.DataFrame):
                raise ValueError(f"[RECONCILE] Expected pandas DataFrame for FR at index {idx}, got {type(fr_df)}")
            if not isinstance(agenda_df, pd.DataFrame):
                raise ValueError(f"[RECONCILE] Expected pandas DataFrame for Agenda at index {idx}, got {type(agenda_df)}")

            try:
                fn = self.get_processing_func()
                reconciled = fn(fr_df, agenda_df)
                out_frames.append(reconciled)

                # Generate output name based on FR filename
                # Replace "Cleaned" with "Finalized"
                # Example: "FR 24_25 S2 Cleaned" -> "FR 24_25 S2 Finalized"
                if "Cleaned" in fr_filename:
                    output_name = fr_filename.replace("Cleaned", "Finalized")
                else:
                    # Fallback: append "Finalized" if "Cleaned" not found
                    output_name = f"{fr_filename} Finalized"

                out_names.append(output_name)
                self._log(f"[RECONCILE] Reconciled FR and Agenda data via {fn.__name__}", reporting)
                self._log(f"[RECONCILE] Output name: {output_name}", reporting)
            except Exception as e:
                self._log(f"[RECONCILE] Failed reconciliation: {e}", reporting)
                raise

        return out_frames, out_names

    # --------------------------
    # Dispatcher
    # --------------------------
    def dispatch(self, dfs: Iterable, names: Iterable[str], reporting: bool = False, **kwargs) -> Tuple[List[pd.DataFrame], List[str]]:
        proc = self.processors.get(self.get_type())
        if not proc:
            raise ValueError(f"Unknown process type: {self.get_type()}")
        return proc(dfs, names, reporting=reporting, **kwargs)


# --------------------------
# Backward-compatible function
# --------------------------
def process(files: Dict, process_type: str) -> Dict[str, pd.DataFrame]:
    """
    Backward-compatible function expected by Pipeline/workflow.py.
    Wraps ASUCProcessor.dispatch and returns dict[name: DataFrame].
    """
    proc = ASUCProcessor(process_type)
    names = list(files.keys())
    dfs = list(files.values())

    processed_dfs, processed_names = proc.dispatch(dfs, names, reporting=False)

    # Stitch results into {name: df}
    results: Dict[str, pd.DataFrame] = {}
    for name, df in zip(processed_names, processed_dfs):
        results[name] = df
    return results

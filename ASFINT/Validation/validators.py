from pathlib import Path
import pandas as pd

from ASFINT.Pipeline.workflow import pull
from ASFINT.Pipeline.workflow import process
from ASFINT.Config.config import get_naming

KEY = {
    'FR': {
        'columns': ['Appx.', 'Org Name', 'Request Type', 'Org Type (year)', 'Amount Requested', 'Amount Allocated', 'Funding Source Primary', 'Contact Email Address'], 
    }
}

def check_file(raw: str|pd.DataFrame, clean: pd.DataFrame, name: str, process_type: str):
    assert process_type in KEY, f"Inputted process_type '{process_type}' not supported, choose from {list(KEY.keys())}"
    assert list(clean.columns) == KEY[process_type]['columns']

    assert get_naming(process_type=process_type, tag='clean file name') in name
    assert get_naming(process_type=process_type, tag='clean file tag') in name


def check(path: str|Path, process_type: str):
    raws = pull(path=path, process_type=process_type)
    cleans = process(files=raws, process_type=process_type)

    raw_lst = list(raws.values())
    for i, (name, file) in enumerate(cleans.items()):
        check_file(raw=raw_lst[i], clean=file, name=name, process_type=process_type) # brittle but works for now
    return
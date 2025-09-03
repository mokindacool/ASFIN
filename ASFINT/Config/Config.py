from ASFINT.Push.pushers import *
from ASFINT.Pull.pullers import *

from typing import Callable

PROCESS_TYPES = {
    'ABSA': {
        'pull': pull_csv, 
        'push': None, 
        'process': None
    }
}

def get_pushpull(process_type: str, func: str) -> Callable: 
    if process_type.strip().upper() in PROCESS_TYPES: 
        fields = PROCESS_TYPES[process_type]
        func = fields.get('func', None)
        if func is None:
            raise ValueError(f"Inputted function {func} not supported, select from: {fields.keys()}")
        else:
            return func
    else:
        raise ValueError(f"Inputted process type {process_type} not supported, select from: {process_type.keys()}")
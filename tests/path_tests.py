import sys
print(f"Sys path: {sys.path}")

def get_my_python_path():
    
    import sys
    PATHS = sys.path
    
    num = 1
    print('\nMy PYTHONPATH: Where Python searches when importing modules (lower number takes precedence):')
    print('-'*91)
    
    for path in PATHS:
        print('{}. {}'.format(num, path))
        num += 1
        
get_my_python_path()

try:
    import AEOCFO.Utility.Cleaning as cl
    from AEOCFO.Utility.Cleaning import in_df
    from AEOCFO.Utility import is_type
    print("Cleaning.py works!")
except Exception as e:
    raise e

try:
    import AEOCFO.Utility.Utils as ut
    from AEOCFO.Utility.Utils import heading_finder
    from AEOCFO.Utility import column_converter
    print("Utils.py works!")
except Exception as e:
    raise e

try:
    from AEOCFO.Transform.Agenda_Processor import *
    from AEOCFO.Transform.Agenda_Processor import ABSA_Processor
    print("Core works!")
except Exception as e:
    raise e

try:
    from AEOCFO.Transform.Processor import ASUCProcessor
    from AEOCFO.Transform import ASUCProcessor
    print("Processor.py works!")
except Exception as e:
    raise e

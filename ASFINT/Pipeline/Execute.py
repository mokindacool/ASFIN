from ASFIN.Utility.Logger_Utils import get_logger
from ASFIN.Pipeline.Drive_Process import drive_process
from ASFIN.Config.Folders import get_folder_id, get_dataset_ids
from ASFIN.Extract.Drive_Pull import drive_pull
from ASFIN.Load.BQ_Push import bigquery_push
from ASFIN.Config.Drive_Config import get_process_config

def execute(t, verbose=True, local=False, local_paths=None, drive=True, bigquery=False, testing=False, haltpush=False):
    """
    t (str): Processing type (eg. Contingency, OASIS, FR, etc).
    verbose (bool): Specifies whether or not to print logs fully.
    drive (bool): specifies whether or not run processing of raw files to a clean file in google drive
    bigquery (bool): specifies whether or not to 
    haltpush (bool): tells the function not to push files (helpful for debugging just pulling and processing functionalities)
    """
    assert t in get_process_config(), f"Inputted type '{t}' not supported. Supported types include: {get_process_config().keys()}"
    
    logger = get_logger(t)
    logger.info(f"--- START PIPELINE: '{t}' ---")
    if verbose: print(f"--- START PIPELINE: '{t}' ---")

    if drive:
        INPUT_folderID, OUTPUT_folderID = get_folder_id(process=t, request='both', testing=testing)   
        folder_ids = {
            'input': INPUT_folderID, 
            'output': OUTPUT_folderID
        }
        drive_process(directory_ids=folder_ids, process_type=t, duplicate_handling="Ignore", reporting=verbose, testing=testing, haltpush=haltpush)

    if bigquery:
        logger.info(f"--- BEGINNING BIG QUERY PIPELINE: '{t} ---")
        if verbose: print(f"--- BEGINNING BIG QUERY PIPELINE: '{t} ---")

        DESTINATION_datasetID = get_dataset_ids(process_type=t, testing=testing)
        dataframes, names = drive_pull(OUTPUT_folderID, process_type="BIGQUERY", reporting=verbose)
        if not dataframes and not names:
            logger.warning(f"No files of query type {t} found in folder ID {OUTPUT_folderID}, ending workloop.")
            if verbose: print(f"No files of query type {t} found in folder ID {OUTPUT_folderID}, ending workloop.")
        df_list = dataframes.values()
        name_list = names.values()
        bigquery_push(DESTINATION_datasetID, df_list, name_list, processing_type=t, duplicate_handling="replace", reporting=verbose)

        logger.info(f"--- ENDING BIG QUERY PIPELINE: '{t} ---")
        if verbose: print(f"--- ENDING BIG QUERY PIPELINE: '{t} ---")

    if local:
        if local_paths is None:
            local_paths = {
                'load path' : "", 
                'save path' : ""
                           }

    logger.info(f"--- END PIPELINE: '{t}' ---\n")
    if verbose: print(f"--- END PIPELINE: '{t}' ---\n")
    
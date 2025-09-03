import numpy as np
import pandas as pd

from ASFINT.Utility.Cleaning import in_df
from ASFINT.Utility.Utils import column_converter, heading_finder, column_renamer

def _year_adder(df_list, year_list, year_rank):
        #private
        """
        Takes a list of dataframes and a corresponding list of years, 
        then mutates those dataframes with a year column containing the year in a element-wise fashion
        """

        for i in range(len(df_list)):
            df_list[i]['Year'] = np.full(df_list[i].shape[0], year_list[i])
            df_list[i]['Year Rank'] = np.full(df_list[i].shape[0], year_rank[i])

def year_adder(df_list, year_list, year_rank):
    return _year_adder(df_list, year_list, year_rank)

def year_rank_collision_handler(df, existing):
    """For re-adjusting year rank via comparing academic year columns that have values formatted "2023-2024".
    Just remaps the Year and Year Rank columns, can handle extra columns."""
    assert in_df(['Year', 'Year Rank'], df), 'Year and Year Rank not in df.'
    assert in_df(['Year', 'Year Rank'], existing), 'Year and Year Rank not in existing.'
    df_cop = df.copy()
    existing_cop = existing.copy()
    
    all_academic_years = pd.concat([existing_cop['Year'], df_cop['Year']]).unique()
    in_order = sorted(all_academic_years, key=lambda x: int(x.split('-')[1]))

    years_to_rank = {year: rank for rank, year in enumerate(in_order)}

    df_cop['Year Rank'] = df_cop['Year'].map(years_to_rank)
    existing_cop['Year Rank'] = existing_cop['Year'].map(years_to_rank)

    return df_cop, existing_cop

def OASIS_Abridged(df, year, name_var = None, rename=None, col_types=None, existing=None) -> pd.DataFrame:
    """
    Expected Intake: 
    - Df with following columns: ['Org ID', 'Organization Name', 'All Registration Steps Completed?',
       'Reg Form Progress\n\n (Pending means you need to wait for OASIS Staff to approve your Reg form)',
       'Number of Signatories\n(Need 4 to 8)', 'Completed T&C', 'Org Type',
       'Callink Page', 'OASIS RSO Designation', 'OASIS Center Advisor ',
       'Year', 'Year Rank']
    - existing_df: already cleaned version of OASIS dataset
    - col_types: a dictionary mapping data types to column names, thus assigning certain/validating columns to have certain types

    - year (str): Details teh academic year

    EXTRA COLUMNS ARE HANDLED BY JUST CONCATING AND LETTING NAN VALUES BE.
    """
    extract_cols = ['Org ID', 'Organization Name', 'OASIS RSO Designation']
    all_cols = ['Org ID', 'Organization Name', 'All Registration Steps Completed?',
       'Reg Form Progress\n\n (Pending means you need to wait for OASIS Staff to approve your Reg form)',
       'Number of Signatories\n(Need 4 to 8)', 'Completed T&C', 'Org Type',
       'Callink Page', 'OASIS RSO Designation', 'OASIS Center Advisor ',
       'Year', 'Year Rank']
    kept_cols = ['Org ID', 'Organization Name', 'OASIS RSO Designation', 'Blue Heart', 'Active', 'Year'] # only first 3 names naturally exist in the df, the rest are added after transformations

    if name_var is None: #phase 0
        df.columns = df.columns.str.strip() # Is the the actual syntax? I forgot
        name_var = dict(zip(all_cols, [[]]*len(all_cols))) # list out alterate names by which the columns are sometimes named as 
        for name in name_var.keys():
            if name == 'OASIS RSO Designation': # cases defining alternate namings of columns e know of
                name_var['OASIS RSO Designation'] = ['LEAD Center Advisor', 'Org Category']

    if kept_cols[0] not in df.columns:
        cleaned_df = heading_finder(df, 0, kept_cols[0]) #phase 1
    else:
        cleaned_df = df.copy()
    
    cleaned_df['Year'] = year #phase 3: there is no info on the df that allows us to parse academic year, it must be fed in
    
    if col_types is None: #phase 4
        
        OClean_Str_Cols = [] # for now the only columns we need to extract from an unclean OASIS csv file are its string columns
        for name in extract_cols: # need to handle for columns detailing the same kind of info being named differently across records from different years
            if name in cleaned_df.columns:
                OClean_Str_Cols.append(name)
            else:
                if name_var[name] == []:
                    raise ValueError(f"Column {name} is missing from inputted dataframe")
                found_alt = False
                for alt in name_var[name]:
                    if alt in cleaned_df.columns:
                        found_alt = True
                        OClean_Str_Cols.append(alt)
                        break
                if not found_alt:
                    raise ValueError(f"Column {name} and alternatives {name_var[name]} are missing from inputted dataframe")
        cleaned_df = column_converter(df=cleaned_df, cols=OClean_Str_Cols, t=str, mutate = False)
    else:
        #expecting col_types to be 
        for key in col_types.keys(): 
            column_converter(df=cleaned_df, cols=col_types[key], t=key, mutate = True)
    
    cleaned_df['Active'] = cleaned_df['Org Type'].apply(lambda x: True if x == 'Registered Student Organizations' else False) #phase 5

    designation_names = ['OASIS RSO Designation'] + name_var['OASIS RSO Designation']
    for name in designation_names:
        if name in cleaned_df:
            cleaned_df['OASIS RSO Designation'] = cleaned_df[name].str.extract(r'(?:LEAD|OASIS) Center Category: (.*)') #phase 6
            break

    cleaned_df['Blue Heart'] = cleaned_df['Organization Name'].str.contains('💙')

    cleaned_df = cleaned_df[kept_cols]
    
    if existing is not None: #phase 8(O): concating onto an existing OASIS dataset
        assert in_df(kept_cols, existing), f"Columns {kept_cols} expected to be in 'existing' df but not found."
        cleaned_df = pd.concat([cleaned_df, existing]).sort_values(by=['Year Rank', 'Organization Name'])
        return cleaned_df
    else: 
        return cleaned_df



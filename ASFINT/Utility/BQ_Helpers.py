import pandas as pd
import re

def clean_name(name: str) -> str:
    # Replace spaces with underscores
    name = name.replace(" ", "_")
    # Remove special characters (allow only letters, numbers, underscores)
    name = re.sub(r'[^a-zA-Z0-9_]', '', name)
    # Ensure it starts with a letter or underscore
    if not re.match(r'^[a-zA-Z_]', name):
        name = "_" + name
    # Truncate to 300 characters
    return name[:300]

def col_name_conversion(dfs) -> list[pd.DataFrame]:
    if isinstance(dfs, pd.DataFrame):
        dfs = [dfs]

    assert all(isinstance(df, pd.DataFrame) for df in dfs), (
        f"dfs input should be a dataframe or list of dataframes but received: {type(dfs)}"
    )

    cleaned_dfs = []
    for df in dfs:
        cleaned_df = df.copy()
        cleaned_df.columns = [clean_name(col) for col in df.columns]
        cleaned_dfs.append(cleaned_df)

    return cleaned_dfs


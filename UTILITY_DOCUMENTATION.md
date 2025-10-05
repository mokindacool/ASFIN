# ASFINT Utility Functions Documentation

## Overview

This document provides comprehensive documentation for the utility functions in the ASFINT system. These utilities provide core functionality for data manipulation, validation, cleaning, and file operations across all processors.

## Utility Modules

### 1. Utils Module (`ASFINT/Utility/Utils.py`)

#### Core Data Manipulation Functions

##### `column_converter(df, dict=None, cols=None, t=None, fillna_val=None, mutate=False, date_varies=False)`
Converts DataFrame columns to specified data types with comprehensive type support.

**Parameters**:
- `df` (pd.DataFrame): Input DataFrame
- `dict` (dict): Batch conversion mapping `{type_str: [col1, col2, ...]}`
- `cols` (list): Single column or list of columns to convert
- `t` (type): Target data type for `cols`
- `fillna_val` (any): Value to fill NaN values after conversion
- `mutate` (bool): If True, modifies DataFrame in-place
- `date_varies` (bool): Handle mixed datetime formats per cell

**Supported Types**:
```python
TYPE_MAP = {
    "int": int,
    "float": float,
    "str": str,
    "timestamp": pd.Timestamp
}
```

**Usage Examples**:
```python
# Single column conversion
df = column_converter(df, cols=['Amount'], t=int, fillna_val=0)

# Batch conversion
df = column_converter(df, dict={
    'str': ['Org Name', 'Description'],
    'int': ['Amount', 'ID'],
    'timestamp': ['Date']
})

# Handle mixed date formats
df = column_converter(df, cols=['Date'], t=pd.Timestamp, date_varies=True)
```

**Features**:
- Automatic NaN handling with type-appropriate defaults
- Mixed datetime format support
- Batch processing for multiple columns
- Non-destructive operation by default

##### `column_renamer(df, rename)`
Standardizes column renaming with support for predefined schemes.

**Parameters**:
- `df` (pd.DataFrame): Input DataFrame
- `rename` (str/dict): Renaming scheme or custom mapping

**Predefined Schemes**:
- `'OASIS-Standard'`: Standardizes OASIS column names

**Custom Mapping**:
```python
# Dictionary mapping old names to new names
rename_dict = {
    'Old Column Name': 'New Column Name',
    'Another Old Name': 'Another New Name'
}
```

**Usage Examples**:
```python
# Use predefined scheme
df = column_renamer(df, 'OASIS-Standard')

# Custom renaming
df = column_renamer(df, {
    'Organization Name': 'Org Name',
    'Registration Date': 'Reg Date'
})
```

##### `heading_finder(df, start_col, start, nth_start=0, shift=0, start_logic='exact', end_col=None, end=None, nth_end=0, end_logic='exact')`
Advanced DataFrame section extraction with flexible matching logic.

**Parameters**:
- `df` (pd.DataFrame): Input DataFrame
- `start_col` (str/int): Column to search for start keyword
- `start` (str): Keyword to start extraction
- `nth_start` (int): Which occurrence to use if multiple matches
- `shift` (int): Rows to shift down from start position
- `start_logic` (str): Matching logic ('exact', 'contains')
- `end_col` (str/int): Column to search for end keyword
- `end` (str/list): Keyword(s) to end extraction
- `nth_end` (int): Which occurrence to use for end
- `end_logic` (str): End matching logic ('exact', 'contains')

**Matching Logic Options**:
- `'exact'`: Exact string match
- `'contains'`: Substring match

**Usage Examples**:
```python
# Basic section extraction
result = heading_finder(df, start_col=0, start="Appx")

# Extract with end condition
result = heading_finder(
    df, 
    start_col=0, 
    start="Section Name",
    end="SUBTOTAL",
    start_logic="exact",
    end_logic="contains"
)

# Multiple end conditions
result = heading_finder(
    df,
    start_col=0,
    start="Data",
    end=["END", "STOP", "FINISH"],
    end_logic="exact"
)
```

**Features**:
- Flexible start/end positioning
- Multiple occurrence handling
- Row shifting for header adjustment
- Automatic column header setting

##### `oasis_cleaner(OASIS_master, approved_orgs_only=True, year=None, club_type=None)`
Comprehensive OASIS dataset cleaning with filtering capabilities.

**Parameters**:
- `OASIS_master` (pd.DataFrame): Master OASIS dataset
- `approved_orgs_only` (bool): Filter for active organizations only
- `year` (str/int/float/list): Year(s) to filter by
- `club_type` (str): Specific club type filter

**Year Filtering**:
- String: Academic year (e.g., '2023-2024')
- Integer/Float: Year rank (e.g., 2023)
- List: Multiple years

**Usage Examples**:
```python
# Basic cleaning
cleaned = oasis_cleaner(oasis_df)

# Filter by year
cleaned = oasis_cleaner(oasis_df, year='2023-2024')

# Filter by multiple years
cleaned = oasis_cleaner(oasis_df, year=['2022-2023', '2023-2024'])

# Filter by club type
cleaned = oasis_cleaner(oasis_df, club_type='Registered Student Organizations')

# Combined filters
cleaned = oasis_cleaner(
    oasis_df,
    approved_orgs_only=True,
    year=2023,
    club_type='Club'
)
```

**Features**:
- Automatic column dropping
- Flexible year filtering
- Organization status filtering
- Club type filtering

##### `ensure_folder(path)`
Creates directory if it doesn't exist.

**Parameters**:
- `path` (Path): Directory path to create

**Usage**:
```python
from pathlib import Path

# Create single directory
ensure_folder(Path("output/cleaned_data"))

# Create nested directories
ensure_folder(Path("output/2024/fr_reports"))
```

---

### 2. Cleaning Module (`ASFINT/Utility/Cleaning.py`)

#### Data Validation Functions

##### `is_type(inpt, t, report=False)`
Comprehensive type checking for single values and iterables.

**Parameters**:
- `inpt` (any): Input value or iterable to check
- `t` (type/tuple): Target type(s) to validate against
- `report` (bool): Enable detailed reporting

**Usage Examples**:
```python
# Single value checking
assert is_type(5, int)  # True
assert is_type("hello", str)  # True
assert is_type(5.0, (int, float))  # True

# Iterable checking
assert is_type([1, 2, 3], int)  # True
assert is_type(["a", "b", "c"], str)  # True
assert is_type([1, "hello", 3.0], (int, str, float))  # True

# Mixed type checking
assert is_type([1, 2, 3], int)  # True
assert is_type([1, "hello"], int)  # False
```

**Features**:
- Single value and iterable support
- Multiple type validation
- Empty iterable handling
- Detailed error reporting

##### `in_df(inpt, df)`
Validates column existence in DataFrame.

**Parameters**:
- `inpt` (str/int/list): Column name, index, or list of either
- `df` (pd.DataFrame): DataFrame to check against

**Usage Examples**:
```python
# Single column check
assert in_df('Amount', df)  # True if 'Amount' column exists
assert in_df(0, df)  # True if DataFrame has at least 1 column

# Multiple column check
assert in_df(['Name', 'Amount'], df)  # True if both columns exist
assert in_df([0, 1, 2], df)  # True if DataFrame has at least 3 columns
```

**Features**:
- String and integer column references
- Single and multiple column validation
- Index bounds checking
- Comprehensive error messages

##### `any_in_df(inpt, df)`
Checks if at least one column exists in DataFrame.

**Parameters**:
- `inpt` (str/list): Column name or list of column names
- `df` (pd.DataFrame): DataFrame to check against

**Usage Examples**:
```python
# Single column
assert any_in_df('Amount', df)  # True if 'Amount' exists

# Multiple columns (at least one must exist)
assert any_in_df(['Amount', 'Total', 'Sum'], df)  # True if any exist
```

**Features**:
- Partial column matching
- String-only validation
- Efficient existence checking

##### `any_drop(df, cols)`
Safely drops columns that exist in DataFrame.

**Parameters**:
- `df` (pd.DataFrame): Input DataFrame
- `cols` (str/list): Column name(s) to drop

**Usage Examples**:
```python
# Drop single column
df = any_drop(df, 'Unwanted Column')

# Drop multiple columns
df = any_drop(df, ['Col1', 'Col2', 'Col3'])

# Safe dropping (ignores non-existent columns)
df = any_drop(df, ['Existing Col', 'Non-existent Col'])
```

**Features**:
- Safe column removal
- Ignores non-existent columns
- Single and multiple column support
- Non-destructive operation

##### `is_valid_iter(inpt, exclude=None)`
Validates if input is a valid iterable with indexing support.

**Parameters**:
- `inpt` (any): Input to validate
- `exclude` (type/list): Types to exclude from validation

**Usage Examples**:
```python
# Basic validation
assert is_valid_iter([1, 2, 3])  # True
assert is_valid_iter("hello")  # True (string is iterable)
assert is_valid_iter(123)  # False (int is not iterable)

# With exclusions
assert is_valid_iter("hello", exclude=str)  # False
assert is_valid_iter([1, 2, 3], exclude=str)  # True
```

**Features**:
- Iterable validation
- Indexing capability check
- Type exclusion support
- String handling

---

### 3. Logger Utils Module (`ASFINT/Utility/Logger_Utils.py`)

#### Logging Functions

##### `get_logger(name)`
Creates and configures logger instances.

**Parameters**:
- `name` (str): Logger name

**Usage**:
```python
from ASFINT.Utility.Logger_Utils import get_logger

logger = get_logger("FR_Processor")
logger.info("Processing started")
logger.error("Processing failed")
```

**Features**:
- Standardized logging configuration
- File and console output
- Timestamp formatting
- Log level management

---

### 4. BQ Helpers Module (`ASFINT/Utility/BQ_Helpers.py`)

#### BigQuery Integration Functions

**Note**: This module provides utilities for Google BigQuery integration, including:
- Data upload functions
- Query execution helpers
- Schema validation
- Connection management

**Usage**:
```python
from ASFINT.Utility.BQ_Helpers import upload_to_bq

# Upload processed data to BigQuery
upload_to_bq(cleaned_df, "project.dataset.table")
```

---

## Common Usage Patterns

### 1. Data Validation Pipeline
```python
from ASFINT.Utility.Cleaning import is_type, in_df, any_in_df

def validate_dataframe(df, required_columns, expected_types):
    """Comprehensive DataFrame validation"""
    
    # Check required columns exist
    assert in_df(required_columns, df), f"Missing required columns: {required_columns}"
    
    # Check data types
    for col, expected_type in expected_types.items():
        assert is_type(df[col], expected_type), f"Column {col} has wrong type"
    
    return True
```

### 2. Safe Data Processing
```python
from ASFINT.Utility.Utils import column_converter, any_drop
from ASFINT.Utility.Cleaning import any_in_df

def safe_data_processing(df):
    """Process data with error handling"""
    
    # Remove unwanted columns safely
    unwanted_cols = ['Temp', 'Unused', 'Old']
    df = any_drop(df, unwanted_cols)
    
    # Convert columns with error handling
    try:
        df = column_converter(df, dict={
            'int': ['Amount', 'ID'],
            'str': ['Name', 'Description']
        })
    except Exception as e:
        print(f"Type conversion error: {e}")
        return None
    
    return df
```

### 3. Section Extraction Pattern
```python
from ASFINT.Utility.Utils import heading_finder

def extract_sections(df, section_configs):
    """Extract multiple sections from DataFrame"""
    
    sections = []
    for config in section_configs:
        try:
            section = heading_finder(
                df=df,
                start_col=config['start_col'],
                start=config['start_keyword'],
                end=config.get('end_keyword'),
                start_logic=config.get('start_logic', 'exact'),
                end_logic=config.get('end_logic', 'exact')
            )
            sections.append(section)
        except Exception as e:
            print(f"Failed to extract section {config['start_keyword']}: {e}")
    
    return sections
```

### 4. Data Cleaning Pipeline
```python
from ASFINT.Utility.Utils import column_converter, column_renamer
from ASFINT.Utility.Cleaning import any_drop

def clean_dataframe(df, cleaning_config):
    """Apply comprehensive data cleaning"""
    
    # Step 1: Remove unwanted columns
    if 'drop_columns' in cleaning_config:
        df = any_drop(df, cleaning_config['drop_columns'])
    
    # Step 2: Rename columns
    if 'rename_columns' in cleaning_config:
        df = column_renamer(df, cleaning_config['rename_columns'])
    
    # Step 3: Convert data types
    if 'type_conversions' in cleaning_config:
        df = column_converter(df, dict=cleaning_config['type_conversions'])
    
    # Step 4: Handle missing values
    if 'fillna_values' in cleaning_config:
        df = df.fillna(cleaning_config['fillna_values'])
    
    return df
```

## Error Handling Best Practices

### 1. Validation with Informative Errors
```python
def validate_input(df, required_cols):
    """Validate input with detailed error messages"""
    
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected DataFrame, got {type(df)}")
    
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    return True
```

### 2. Graceful Degradation
```python
def safe_column_conversion(df, col, target_type, default_value=None):
    """Safely convert column with fallback"""
    
    try:
        return column_converter(df, cols=[col], t=target_type)
    except Exception as e:
        print(f"Conversion failed for {col}: {e}")
        if default_value is not None:
            df[col] = default_value
        return df
```

### 3. Comprehensive Error Logging
```python
from ASFINT.Utility.Logger_Utils import get_logger

logger = get_logger("DataProcessing")

def process_with_logging(df):
    """Process data with comprehensive logging"""
    
    logger.info(f"Starting processing of {len(df)} rows")
    
    try:
        # Processing steps
        result = process_data(df)
        logger.info("Processing completed successfully")
        return result
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        logger.debug(f"DataFrame info: {df.info()}")
        raise
```

## Performance Optimization

### 1. Efficient Type Checking
```python
# Use is_type for bulk validation
if is_type(df['Amount'], (int, float)):
    # Process numeric data
    pass

# Avoid individual type checks in loops
valid_rows = is_type(df['Status'], str)
df_clean = df[valid_rows]
```

### 2. Vectorized Operations
```python
# Use column_converter for batch operations
df = column_converter(df, dict={
    'int': ['ID', 'Amount', 'Count'],
    'str': ['Name', 'Description']
})

# Avoid individual column processing
```

### 3. Memory-Efficient Processing
```python
# Use mutate=True for in-place operations when possible
df = column_converter(df, cols=['Amount'], t=int, mutate=True)

# Process in chunks for large datasets
chunk_size = 10000
for chunk in pd.read_csv('large_file.csv', chunksize=chunk_size):
    processed_chunk = process_chunk(chunk)
    # Handle processed chunk
```

## Testing Utilities

### 1. Test Data Generation
```python
def create_test_dataframe():
    """Create standardized test DataFrame"""
    return pd.DataFrame({
        'ID': range(1, 101),
        'Name': [f'Item_{i}' for i in range(1, 101)],
        'Amount': [i * 10 for i in range(1, 101)],
        'Date': pd.date_range('2024-01-01', periods=100)
    })
```

### 2. Validation Testing
```python
def test_utility_functions():
    """Test utility functions with various inputs"""
    
    # Test is_type
    assert is_type([1, 2, 3], int)
    assert not is_type([1, "hello"], int)
    
    # Test in_df
    df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    assert in_df('A', df)
    assert in_df(['A', 'B'], df)
    assert not in_df('C', df)
    
    # Test column_converter
    df = pd.DataFrame({'Amount': ['100', '200', '300']})
    result = column_converter(df, cols=['Amount'], t=int)
    assert result['Amount'].dtype == 'int64'
```

## Conclusion

The ASFINT utility functions provide a comprehensive toolkit for data manipulation, validation, and processing. These utilities are designed to be:

- **Robust**: Comprehensive error handling and validation
- **Flexible**: Support for various data types and formats
- **Efficient**: Optimized for performance with large datasets
- **Consistent**: Standardized interfaces across all functions
- **Extensible**: Easy to extend and customize for specific needs

By following the patterns and best practices outlined in this documentation, developers can effectively utilize these utilities to build reliable and maintainable data processing pipelines.

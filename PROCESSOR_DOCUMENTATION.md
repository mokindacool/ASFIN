# ASFINT Processor Documentation

## Overview

This document provides detailed documentation for each processor type in the ASFINT system. Each processor is designed to handle specific types of financial and organizational data with specialized cleaning and transformation logic.

## Processor Architecture

All processors follow a common pattern:
1. **Input Validation**: Verify data format and structure
2. **Data Extraction**: Extract relevant sections using keywords/patterns
3. **Data Transformation**: Apply cleaning and standardization rules
4. **Output Generation**: Return cleaned DataFrame and metadata

## Processor Types

### 1. FR Processor (Financial Reports)

**File**: `ASFINT/Transform/FR_Processor.py`

#### Purpose
Processes Financial Reports (FR) spreadsheets that contain budget allocations organized by alphabetical sections.

#### Key Functions

##### `FR_ProcessorV2(df, txt, date_format="%m/%d/%Y", debug=False)`
Main processing function for FR data.

**Parameters**:
- `df` (pd.DataFrame): Raw FR spreadsheet data
- `txt` (str): Text content of the file for date extraction
- `date_format` (str): Expected date format (default: "%m/%d/%Y")
- `debug` (bool): Enable debug output

**Returns**:
- `tuple`: (cleaned_dataframe, extracted_date)

**Processing Logic**:
1. **Date Extraction**: Uses regex to find dates in text content
   - Supports formats: MM/DD/YYYY, YYYY-MM-DD
   - Defaults to "00/00/0000" if no date found
2. **Section Extraction**: Uses `FR_Helper` to extract data sections
3. **Alphabet Filtering**: Filters data using FY24 alphabet system:
   - Primary: A-Z (26 letters)
   - Extended: AA-AZ (26 combinations)
   - Double: BB-ZZ (25 combinations)
   - Total: 77 possible sections

##### `FR_Helper(df, given_start='Appx', start_col=0, adding_end_keyword='END', end_col=0, alphabet=None, nth_occurence=1, reporting=False)`
Helper function for extracting data sections.

**Parameters**:
- `df` (pd.DataFrame): Input data
- `given_start` (str): Keyword to start extraction (default: 'Appx')
- `start_col` (int/str): Column to search for start keyword
- `alphabet` (list): List of valid section identifiers
- `nth_occurence` (int): Which occurrence to use if multiple matches

**Processing Logic**:
1. Uses `heading_finder` to locate "Appx" section
2. Filters rows based on alphabet list
3. Stops at first NaN row if no alphabet provided
4. Returns filtered DataFrame

#### Example Usage
```python
from ASFINT.Transform.FR_Processor import FR_ProcessorV2
import pandas as pd

# Load FR data
df = pd.read_csv("fr_report.csv")
with open("fr_report.csv", 'r') as f:
    text_content = f.read()

# Process data
cleaned_df, date = FR_ProcessorV2(df, text_content)
print(f"Processed {len(cleaned_df)} rows, date: {date}")
```

#### Output Format
- **File naming**: `Ficomm-Reso_YYYY-MM-DD_XXX_GF.csv`
- **Data structure**: Rows filtered by alphabet sections (A-Z, AA-AZ, BB-ZZ)
- **Date format**: MM/DD/YYYY

---

### 2. ABSA Processor (Associated Students Business Affairs)

**File**: `ASFINT/Transform/ABSA_Processor.py`

#### Purpose
Processes ABSA (Associated Students Business Affairs) data with categorized organization types and budget information.

#### Key Functions

##### `ABSA_Processor(df, Cats=None, Drop=None, Add=None)`
Main processing function for ABSA data.

**Parameters**:
- `df` (pd.DataFrame): Raw ABSA spreadsheet data
- `Cats` (dict): Custom categorization dictionary
- `Drop` (str/list): Categories to exclude from processing
- `Add` (str): Categories to add (TODO: not implemented)

**Default Categories**:
```python
Types = {
    'Header': [
        'ASUC Chartered Programs and Commissions',
        'Publications (PUB) Registered Student Organizations',
        'Student Activity Groups (SAG)',
        'Student-Initiated Service Group (SISG)'
    ],
    'No Header': [
        'Office of the President',
        'Office of the Executive Vice President',
        'Office of External Affairs Vice President',
        'Office of the Academic Affairs Vice President',
        "Student Advocate's Office",
        'Senate',
        'Appointed Officials',
        'Operations',
        'Elections',
        'External Expenditures'
    ],
    'Final Counts': [
        'ASUC External Budget',
        'ASUC Internal Budget',
        'FY25 GENERAL BUDGET'
    ]
}
```

**Processing Logic**:
1. **Header Categories**: Uses `heading_finder` with `shift=1` to skip header row
2. **No Header Categories**: Uses `heading_finder` with `shift=0` to include header row
3. **Section Extraction**: Extracts data between category name and 'SUBTOTAL'
4. **Data Combination**: Concatenates all extracted sections
5. **Organization Tagging**: Adds 'Org Category' column to identify source section

##### `_dropper(instance, dictionary)`
Helper function to remove categories from processing.

**Parameters**:
- `instance` (str): Category name to remove
- `dictionary` (dict): Categories dictionary to modify

#### Example Usage
```python
from ASFINT.Transform.ABSA_Processor import ABSA_Processor
import pandas as pd

# Load ABSA data
df = pd.read_csv("absa_budget.csv")

# Process with default categories
cleaned_df = ABSA_Processor(df)

# Process with custom categories
custom_cats = {
    'Header': ['ASUC Chartered Programs and Commissions'],
    'No Header': ['Office of the President', 'Senate']
}
cleaned_df = ABSA_Processor(df, Cats=custom_cats)

# Exclude specific categories
cleaned_df = ABSA_Processor(df, Drop='Elections')
```

#### Output Format
- **File naming**: `ABSA_YYYY-MM-DD_GF.csv`
- **Data structure**: Combined data from all categories with 'Org Category' column
- **Columns**: Original columns plus 'Org Category' identifier

---

### 3. OASIS Processor (Student Organizations)

**File**: `ASFINT/Transform/OASIS_Processor.py`

#### Purpose
Processes OASIS (student organization registration) data with year ranking and organization categorization.

#### Key Functions

##### `OASIS_Abridged(df, year, name_var=None, rename=None, col_types=None, existing=None)`
Main processing function for OASIS data.

**Parameters**:
- `df` (pd.DataFrame): Raw OASIS registration data
- `year` (str): Academic year (e.g., "2023-2024")
- `name_var` (dict): Alternative column name mappings
- `rename` (dict): Column renaming rules
- `col_types` (dict): Data type specifications
- `existing` (pd.DataFrame): Existing OASIS data to merge with

**Expected Input Columns**:
```python
all_cols = [
    'Org ID',
    'Organization Name',
    'All Registration Steps Completed?',
    'Reg Form Progress\n\n (Pending means you need to wait for OASIS Staff to approve your Reg form)',
    'Number of Signatories\n(Need 4 to 8)',
    'Completed T&C',
    'Org Type',
    'Callink Page',
    'OASIS RSO Designation',
    'OASIS Center Advisor ',
    'Year',
    'Year Rank'
]
```

**Processing Logic**:
1. **Column Standardization**: Handles alternative column names
2. **Data Type Conversion**: Converts columns to appropriate types
3. **Active Status**: Determines if organization is active based on 'Org Type'
4. **Designation Extraction**: Extracts OASIS RSO designation from text
5. **Blue Heart Detection**: Identifies organizations with blue heart emoji
6. **Year Ranking**: Assigns year rank based on academic year
7. **Data Merging**: Optionally merges with existing dataset

##### `year_rank_collision_handler(df, existing)`
Handles year rank conflicts when merging datasets.

**Parameters**:
- `df` (pd.DataFrame): New data to add
- `existing` (pd.DataFrame): Existing dataset

**Processing Logic**:
1. Combines all academic years from both datasets
2. Sorts years by end year (e.g., "2023-2024" sorted by 2024)
3. Assigns sequential year ranks
4. Updates both datasets with consistent year ranks

##### `year_adder(df_list, year_list, year_rank)`
Adds year information to multiple DataFrames.

**Parameters**:
- `df_list` (list): List of DataFrames
- `year_list` (list): Corresponding academic years
- `year_rank` (list): Corresponding year ranks

#### Example Usage
```python
from ASFINT.Transform.OASIS_Processor import OASIS_Abridged
import pandas as pd

# Load OASIS data
df = pd.read_csv("oasis_registrations.csv")

# Process new data
cleaned_df = OASIS_Abridged(df, year="2023-2024")

# Merge with existing data
existing_df = pd.read_csv("existing_oasis_data.csv")
merged_df = OASIS_Abridged(df, year="2023-2024", existing=existing_df)

# Handle year rank conflicts
from ASFINT.Transform.OASIS_Processor import year_rank_collision_handler
df_updated, existing_updated = year_rank_collision_handler(df, existing_df)
```

#### Output Format
- **File naming**: `OASIS_YYYY-MM-DD_GF.csv`
- **Data structure**: Standardized organization data with year ranking
- **Key columns**: Org ID, Organization Name, OASIS RSO Designation, Blue Heart, Active, Year

---

### 4. Agenda Processor (Meeting Agendas)

**File**: `ASFINT/Transform/Agenda_Processor.py`

#### Purpose
Processes meeting agenda text to extract funding decisions and allocations for student organizations.

#### Key Functions

##### `Agenda_Processor(inpt, start=['Contingency Funding', 'Contingency'], end=['Finance Rule', 'Rule Waiver', 'Space Reservation', 'Sponsorship', 'Adjournment', 'ABSA', 'ABSA Appeals'], identifier='(\w+\s\d{1,2}\w*,\s\d{4})', date_format="%m/%d/%Y", debug=False)`
Main processing function for agenda text.

**Parameters**:
- `inpt` (str): Raw agenda text content
- `start` (list): Keywords to start extraction
- `end` (list): Keywords to end extraction
- `identifier` (str): Regex pattern for date extraction
- `date_format` (str): Expected date format
- `debug` (bool): Enable debug output

**Processing Logic**:
1. **Date Extraction**: Uses regex to find meeting date
2. **Section Extraction**: Extracts contingency funding section
3. **Organization Identification**: Identifies organization names
4. **Motion Processing**: Extracts funding motions and decisions
5. **Decision Classification**: Categorizes decisions as:
   - Approved (with amount)
   - Denied or Tabled Indefinitely
   - Tabled (temporarily)
   - No record
   - Error (unclear decision)

##### `_find_chunk_pattern(starts, ends, end_prepattern='\d\.\s')`
Creates regex pattern for text extraction.

**Parameters**:
- `starts` (list): Start keywords
- `ends` (list): End keywords
- `end_prepattern` (str): Pattern before end keywords

**Returns**: Compiled regex pattern for text extraction

##### `_motion_processor(club_names, names_and_motions)`
Processes organization names and their associated motions.

**Parameters**:
- `club_names` (list): List of organization names
- `names_and_motions` (list): List of all text lines

**Returns**: Dictionary mapping organization names to their motions

#### Example Usage
```python
from ASFINT.Transform.Agenda_Processor import Agenda_Processor

# Load agenda text
with open("meeting_agenda.txt", 'r') as f:
    agenda_text = f.read()

# Process agenda
decisions_df, date = Agenda_Processor(agenda_text, debug=True)

# Custom processing
custom_start = ['Contingency Funding']
custom_end = ['Sponsorship', 'Adjournment']
decisions_df, date = Agenda_Processor(
    agenda_text,
    start=custom_start,
    end=custom_end,
    identifier=r'(\w+\s\d{1,2}\w*,\s\d{4})'
)
```

#### Output Format
- **File naming**: Based on extracted date
- **Data structure**: DataFrame with funding decisions
- **Columns**:
  - `Organization Name`: Name of requesting organization
  - `Ficomm Decision`: Decision category (Approved, Denied, etc.)
  - `Amount Allocated`: Dollar amount (if approved)
  - `Date`: Meeting date

---

## Common Processing Patterns

### 1. Data Extraction
All processors use the `heading_finder` utility to locate and extract relevant data sections:
```python
from ASFINT.Utility.Utils import heading_finder

# Extract section with header
result = heading_finder(
    df=df,
    start_col=0,
    start="Section Name",
    shift=1,  # Skip header row
    end="SUBTOTAL",
    start_logic="exact",
    end_logic="contains"
)
```

### 2. Data Validation
All processors include validation checks:
```python
from ASFINT.Utility.Cleaning import in_df, is_type

# Check column existence
assert in_df(['Required Column'], df), "Required column missing"

# Check data types
assert is_type(df['Amount'], (int, float)), "Amount must be numeric"
```

### 3. Error Handling
Processors include comprehensive error handling:
```python
try:
    result = processor_function(df)
except Exception as e:
    print(f"Processing error: {e}")
    # Handle gracefully or re-raise
```

### 4. Data Type Conversion
Standardized column type conversion:
```python
from ASFINT.Utility.Utils import column_converter

# Convert multiple columns
df = column_converter(
    df=df,
    dict={
        'str': ['Org Name', 'Description'],
        'int': ['Amount', 'ID'],
        'timestamp': ['Date']
    }
)
```

## Testing Processors

### Unit Testing
```python
import pytest
from ASFINT.Transform.FR_Processor import FR_ProcessorV2

def test_fr_processor():
    # Create test data
    test_df = pd.DataFrame({
        'Appx': ['Appx', 'A', 'B', 'C'],
        'Amount': [100, 200, 300, 400]
    })
    
    # Test processing
    result_df, date = FR_ProcessorV2(test_df, "Report 04/12/2024")
    
    # Assertions
    assert len(result_df) == 3  # A, B, C sections
    assert date == "04/12/2024"
    assert 'Appx' in result_df.columns
```

### Integration Testing
```python
def test_processor_integration():
    # Test with real file
    df = pd.read_csv("test_data.csv")
    result = processor_function(df)
    
    # Validate output
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert expected_columns in result.columns
```

## Performance Considerations

### Memory Usage
- Processors use `.copy()` to avoid modifying original data
- Large datasets are processed in chunks when possible
- Memory-efficient concatenation for multiple sections

### Processing Speed
- Uses vectorized pandas operations
- Efficient regex patterns for text processing
- Minimal loops, maximum vectorization

### Error Recovery
- Graceful handling of malformed data
- Detailed error messages for debugging
- Fallback values for missing data

## Extending Processors

### Adding New Processors
1. Create new processor file in `ASFINT/Transform/`
2. Implement processor function following established patterns
3. Add configuration to `ASFINT/Config/Config.py`
4. Create unit tests in `tests/`
5. Update documentation

### Customizing Existing Processors
1. Use optional parameters for customization
2. Maintain backward compatibility
3. Add validation for new parameters
4. Update tests for new functionality

## Conclusion

The ASFINT processor system provides a robust, extensible framework for processing various types of financial and organizational data. Each processor is designed with specific use cases in mind while following common patterns for consistency and maintainability.

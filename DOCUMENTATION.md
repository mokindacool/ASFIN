# ASFINT (ASUC Finance Transformations) - Comprehensive Documentation

## Overview

ASFINT is a Python library designed for cleaning and transforming raw financial data into standardized, cleaned datasets for the OCFO (Office of the Chief Financial Officer) Financial Database. The system implements an ETL (Extract, Transform, Load) pipeline that can process various types of financial documents and spreadsheets.

## Core Architecture

### ETL Pipeline Structure

The system follows a three-stage ETL pipeline:

1. **Extract (Pull)**: Reads raw data files from input directories
2. **Transform (Process)**: Applies data cleaning and transformation logic
3. **Load (Push)**: Saves cleaned data to output directories

```
Input Files → Pull → Process → Push → Cleaned Files
```

### Main Components

#### 1. Pipeline Module (`ASFINT/Pipeline/workflow.py`)
- **`pull(path, process_type)`**: Extracts data from files/directories
- **`process(files, process_type)`**: Transforms raw data using appropriate processor
- **`push(dfs, path, process_type)`**: Saves cleaned data to output location
- **`run(pull_path, push_path, process_type)`**: Orchestrates the entire ETL pipeline

#### 2. Configuration Module (`ASFINT/Config/Config.py`)
Defines supported process types and their configurations:
- **ABSA**: Associated Students Business Affairs data
- **CONTINGENCY**: Contingency fund data
- **OASIS**: Student organization registration data
- **FR**: Financial reports data

Each process type specifies:
- Pull function (how to read data)
- Push function (how to save data)
- Process function (how to transform data)
- Naming conventions for file outputs

#### 3. Transform Module (`ASFINT/Transform/`)
Contains specialized processors for different data types:

##### ASUCProcessor (`Processor.py`)
Main processor wrapper that routes data to appropriate transformation functions based on process type.

##### FR_Processor (`FR_Processor.py`)
Processes Financial Reports:
- Extracts data using `heading_finder` utility
- Handles FY24 alphabet-based sectioning (A-Z, AA-AZ, BB-ZZ)
- Extracts dates from text content
- Returns cleaned DataFrame and extracted date

##### ABSA_Processor (`ABSA_Processor.py`)
Processes Associated Students Business Affairs data with categorization and filtering capabilities.

##### OASIS_Processor (`OASIS_Processor.py`)
Processes student organization registration data with year ranking and collision handling.

##### Agenda_Processor (`Agenda_Processor.py`)
Processes meeting agenda data.

#### 4. Utility Modules

##### Utils (`ASFINT/Utility/Utils.py`)
Core utility functions:
- **`column_converter()`**: Type conversion for DataFrame columns
- **`column_renamer()`**: Standardized column renaming
- **`heading_finder()`**: Locates data sections within spreadsheets
- **`oasis_cleaner()`**: Filters and cleans OASIS data
- **`ensure_folder()`**: Creates directories if they don't exist

##### Cleaning (`ASFINT/Utility/Cleaning.py`)
Data validation and cleaning utilities:
- **`is_type()`**: Type checking for data validation
- **`in_df()`**: Column existence validation
- **`any_in_df()`**: Partial column matching
- **`any_drop()`**: Safe column removal

#### 5. I/O Modules

##### Pull (`ASFINT/Pull/pullers.py`)
- **`pull_csv()`**: Reads CSV files, with special handling for FR files (includes text content)

##### Push (`ASFINT/Push/pushers.py`)
- **`push_csv()`**: Saves DataFrames as CSV files

## Data Flow

### 1. Input Processing
```
Raw Files (CSV/Excel) → pull() → {filename: DataFrame} dictionary
```

### 2. Transformation
```
Raw DataFrames → ASUCProcessor → Cleaned DataFrames + Updated Names
```

### 3. Output Generation
```
Cleaned DataFrames → push() → Cleaned CSV Files
```

## Supported Data Types

### Financial Reports (FR)
- **Purpose**: Process financial report spreadsheets
- **Key Features**:
  - Extracts data sections using "Appx" keyword
  - Handles FY24 alphabet-based organization (A-Z, AA-AZ, BB-ZZ)
  - Extracts dates from file content
  - Outputs: `Ficomm-Reso_YYYY-MM-DD_XXX_GF.csv`

### ABSA (Associated Students Business Affairs)
- **Purpose**: Process business affairs data
- **Features**: Categorization and filtering capabilities
- **Output**: `ABSA_YYYY-MM-DD_GF.csv`

### OASIS (Student Organizations)
- **Purpose**: Process student organization registration data
- **Features**: Year ranking, collision handling, organization filtering
- **Output**: `OASIS_YYYY-MM-DD_GF.csv`

### CONTINGENCY
- **Purpose**: Process contingency fund data
- **Features**: Similar to ABSA processing
- **Output**: `ABSA_YYYY-MM-DD_GF.csv`

## File Naming Conventions

The system uses standardized naming conventions:
- **Raw files**: `[Type]_YYYY-MM-DD_XXX_RF.csv`
- **Cleaned files**: `[Type]_YYYY-MM-DD_XXX_GF.csv`

Where:
- `Type`: Data type (FR, ABSA, OASIS, etc.)
- `YYYY-MM-DD`: Date extracted from content
- `XXX`: Additional identifiers (numbering, coding)
- `RF`: Raw File tag
- `GF`: Good File (cleaned) tag

## Key Features

### 1. Flexible Data Extraction
- Handles both single files and directories
- Supports various file formats (primarily CSV)
- Robust error handling for malformed files

### 2. Intelligent Data Processing
- Context-aware section extraction using keywords
- Date extraction from text content
- Type-safe column operations
- Data validation and cleaning

### 3. Configurable Processing
- Process-type specific configurations
- Extensible architecture for new data types
- Customizable naming conventions

### 4. Error Handling
- Graceful handling of malformed data
- Detailed error reporting
- Validation at multiple stages

## Dependencies

- **pandas**: Data manipulation and analysis
- **numpy**: Numerical operations
- **scikit-learn**: Machine learning utilities (cosine similarity)
- **pyarrow**: Apache Arrow integration
- **pandas-gbq**: Google BigQuery integration
- **rapidfuzz**: Fuzzy string matching
- **tqdm**: Progress bars

## Usage Examples

### Command Line Usage
```bash
# Process FR data
python execute.py --input files/input --output files/output --process fr

# Process ABSA data
python execute.py --input files/input --output files/output --process absa
```

### Programmatic Usage
```python
from ASFINT.Pipeline.workflow import run

# Process FR data programmatically
run(
    pull_path="files/input",
    push_path="files/output", 
    process_type="fr"
)
```

### Direct Processor Usage
```python
from ASFINT.Transform.FR_Processor import FR_ProcessorV2
import pandas as pd

# Process a single FR file
df = pd.read_csv("raw_fr_file.csv")
cleaned_df, date = FR_ProcessorV2(df, "file content text")
```

## Configuration

The system is configured through `ASFINT/Config/Config.py`:

```python
PROCESS_TYPES = {
    'FR': {
        'pull': pull_csv,
        'push': push_csv,
        'process': ASUCProcessor('FR'),
        'naming': {
            'raw tag': "RF",
            'clean tag': "GF", 
            'clean file name': "Ficomm-Reso",
            'date format': "%m/%d/%Y",
            'raw name dependency': ["Date", "Numbering", "Coding"]
        }
    }
    # ... other process types
}
```

## Error Handling

The system includes comprehensive error handling:
- File existence validation
- Data type checking
- Column existence validation
- Graceful degradation for malformed data
- Detailed error messages and logging

## Performance Considerations

- Uses pandas for efficient data manipulation
- Implements lazy loading for large datasets
- Memory-efficient processing of multiple files
- Progress tracking for long-running operations

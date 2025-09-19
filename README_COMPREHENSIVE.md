# ASFINT (ASUC Finance Transformations) - Complete Documentation

## 📋 Documentation Overview

This repository contains comprehensive documentation for the ASFINT system. Below is a complete guide to understanding, using, and testing the codebase.

## 📚 Documentation Structure

### 1. [Main Documentation](DOCUMENTATION.md)
**Complete system overview and architecture**
- System purpose and functionality
- ETL pipeline architecture
- Component descriptions
- Data flow diagrams
- Configuration details
- Usage examples

### 2. [Processor Documentation](PROCESSOR_DOCUMENTATION.md)
**Detailed processor specifications**
- FR Processor (Financial Reports)
- ABSA Processor (Associated Students Business Affairs)
- OASIS Processor (Student Organizations)
- Agenda Processor (Meeting Agendas)
- Processing logic and examples
- Input/output formats

### 3. [Utility Documentation](UTILITY_DOCUMENTATION.md)
**Core utility functions and helpers**
- Data manipulation utilities
- Validation functions
- Cleaning operations
- File I/O helpers
- Common usage patterns

### 4. [Testing Guide](TESTING_GUIDE.md)
**Comprehensive testing and validation methods**
- Unit testing strategies
- Integration testing approaches
- Data validation techniques
- Performance testing
- Manual testing procedures

## 🚀 Quick Start Guide

### Installation
```bash
# Create conda environment
conda create -n ASFINT0.1 python=3.11.8
conda activate ASFINT0.1

# Install dependencies
pip install -r requirements.txt
pip install -e .
```

### Basic Usage

#### Command Line
```bash
# Process FR data
python execute.py --input files/input --output files/output --process fr

# Process ABSA data
python execute.py --input files/input --output files/output --process absa

# Process OASIS data
python execute.py --input files/input --output files/output --process oasis
```

#### Programmatic Usage
```python
from ASFINT.Pipeline.workflow import run

# Process data programmatically
run(
    pull_path="files/input",
    push_path="files/output",
    process_type="fr"
)
```

### File Structure Setup
```
ASFIN/
├── files/
│   ├── input/          # Place raw files here
│   └── output/         # Cleaned files appear here
├── ASFINT/            # Main package
├── tests/             # Test files
└── docs/              # Documentation
```

## 🔧 System Architecture

### ETL Pipeline
```
Input Files → Pull → Process → Push → Cleaned Files
     ↓           ↓        ↓        ↓         ↓
  Raw Data → Extract → Transform → Load → Clean Data
```

### Supported Data Types
- **FR**: Financial Reports with alphabetical sections
- **ABSA**: Associated Students Business Affairs data
- **OASIS**: Student organization registration data
- **CONTINGENCY**: Contingency fund data
- **AGENDA**: Meeting agenda funding decisions

### Key Components
- **Pipeline**: ETL orchestration (`workflow.py`)
- **Processors**: Data transformation logic (`Transform/`)
- **Utilities**: Helper functions (`Utility/`)
- **Configuration**: Process type definitions (`Config/`)

## 🧪 Testing the System

### Run Existing Tests
```bash
# Run all tests
python -m pytest tests/

# Run specific test file
python -m pytest tests/fr_proc_tests.py -v

# Run with coverage
python -m pytest tests/ --cov=ASFINT
```

### Manual Testing
1. **Prepare test data**: Place sample files in `files/input/`
2. **Run processing**: Execute the appropriate processor
3. **Validate output**: Check `files/output/` for cleaned files
4. **Verify results**: Ensure data integrity and format

### Data Validation
```python
# Validate input data
from ASFINT.Utility.Cleaning import in_df, is_type

assert in_df(['Required Column'], df)
assert is_type(df['Amount'], (int, float))

# Validate output data
assert not cleaned_df.empty
assert 'Expected Column' in cleaned_df.columns
```

## 📊 Data Processing Examples

### FR (Financial Reports) Processing
```python
from ASFINT.Transform.FR_Processor import FR_ProcessorV2
import pandas as pd

# Load and process FR data
df = pd.read_csv("fr_report.csv")
with open("fr_report.csv", 'r') as f:
    text_content = f.read()

cleaned_df, date = FR_ProcessorV2(df, text_content)
print(f"Processed {len(cleaned_df)} rows, date: {date}")
```

### ABSA Processing
```python
from ASFINT.Transform.ABSA_Processor import ABSA_Processor

# Process with default categories
cleaned_df = ABSA_Processor(df)

# Custom processing
custom_cats = {
    'Header': ['ASUC Chartered Programs'],
    'No Header': ['Office of the President']
}
cleaned_df = ABSA_Processor(df, Cats=custom_cats)
```

### OASIS Processing
```python
from ASFINT.Transform.OASIS_Processor import OASIS_Abridged

# Process new data
cleaned_df = OASIS_Abridged(df, year="2023-2024")

# Merge with existing data
merged_df = OASIS_Abridged(df, year="2023-2024", existing=existing_df)
```

## 🔍 Ways to Prove System Functionality

### 1. Unit Testing
- Test individual functions with known inputs
- Verify expected outputs
- Check error handling

### 2. Integration Testing
- Test complete ETL pipeline
- Verify data flow between components
- Test with real file formats

### 3. Data Validation
- Compare input vs output data
- Verify data integrity
- Check business rule compliance

### 4. Performance Testing
- Test with large datasets
- Measure processing time
- Monitor memory usage

### 5. Regression Testing
- Ensure consistent results
- Test with historical data
- Verify backward compatibility

### 6. Manual Verification
- Process known datasets
- Compare with expected results
- Validate file naming conventions

## 📁 File Naming Conventions

### Input Files (Raw)
- Format: `[Type]_YYYY-MM-DD_XXX_RF.csv`
- Example: `FR_2024-04-12_001_RF.csv`

### Output Files (Cleaned)
- Format: `[Type]_YYYY-MM-DD_XXX_GF.csv`
- Example: `Ficomm-Reso_2024-04-12_001_GF.csv`

### Tags
- `RF`: Raw File
- `GF`: Good File (cleaned)

## ⚙️ Configuration

### Process Types
Configured in `ASFINT/Config/Config.py`:
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
            'date format': "%m/%d/%Y"
        }
    }
    # ... other types
}
```

### Adding New Process Types
1. Create processor function
2. Add configuration to `PROCESS_TYPES`
3. Update tests
4. Document usage

## 🐛 Troubleshooting

### Common Issues

#### File Not Found
```
Error: Path 'input_file.csv' does not exist.
```
**Solution**: Ensure files are in `files/input/` directory

#### Processing Errors
```
Could not read or process file 'file.csv'
```
**Solution**: Check file format and content structure

#### Missing Columns
```
AssertionError: Required column 'Amount' not found
```
**Solution**: Verify input data has expected columns

### Debug Mode
Enable debug output for detailed processing information:
```python
result_df, date = FR_ProcessorV2(df, text, debug=True)
```

## 📈 Performance Considerations

### Memory Usage
- Processors use `.copy()` to avoid modifying original data
- Large datasets processed efficiently with pandas
- Memory usage scales linearly with data size

### Processing Speed
- Vectorized operations for optimal performance
- Efficient regex patterns for text processing
- Minimal loops, maximum vectorization

### Scalability
- Handles single files and directories
- Supports batch processing
- Configurable for different data volumes

## 🔒 Error Handling

### Graceful Degradation
- Continues processing when possible
- Provides informative error messages
- Logs detailed error information

### Validation
- Input data validation
- Type checking
- Column existence verification

### Recovery
- Fallback values for missing data
- Error recovery procedures
- Data integrity preservation

## 📝 Contributing

### Adding New Features
1. Create feature branch
2. Implement functionality
3. Add comprehensive tests
4. Update documentation
5. Submit pull request

### Code Standards
- Follow existing patterns
- Add type hints
- Include docstrings
- Write tests for new code

## 📞 Support

### Documentation
- Refer to specific documentation files
- Check examples in each module
- Review test files for usage patterns

### Issues
- Check existing issues
- Provide detailed error information
- Include sample data when possible

## 🎯 Key Takeaways

1. **ASFINT is a comprehensive ETL system** for processing financial and organizational data
2. **Modular architecture** allows for easy extension and maintenance
3. **Robust error handling** ensures reliable data processing
4. **Comprehensive testing** validates system functionality
5. **Detailed documentation** supports development and usage

## 📋 Next Steps

1. **Read the main documentation** to understand the system
2. **Review processor documentation** for specific use cases
3. **Study utility functions** for data manipulation needs
4. **Follow the testing guide** to validate functionality
5. **Start with simple examples** and build complexity

---

**For detailed information, refer to the specific documentation files linked above. Each document provides comprehensive coverage of its respective area with examples, best practices, and troubleshooting guidance.**

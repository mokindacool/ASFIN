# ASFINT Testing and Validation Guide

## Overview

This guide provides comprehensive methods to test, validate, and prove the functionality of the ASFINT system. It covers unit testing, integration testing, data validation, and performance testing approaches.

## Testing Strategy

### 1. Unit Testing
Test individual components in isolation to ensure they work correctly.

### 2. Integration Testing
Test the interaction between components and the complete ETL pipeline.

### 3. Data Validation Testing
Verify that data transformations produce correct and expected results.

### 4. Performance Testing
Ensure the system can handle expected data volumes efficiently.

## Existing Test Structure

The codebase includes a `tests/` directory with the following test files:
- `fr_proc_tests.py` - FR Processor tests
- `cleaning_tests.py` - Data cleaning utility tests
- `oasis_proc_tests.py` - OASIS Processor tests
- `path_tests.py` - File path handling tests
- `utils_tests.py` - Utility function tests
- `ag_proc_tests.py` - Agenda Processor tests

## Testing Methods

### 1. Unit Testing Framework

#### Running Existing Tests
```bash
# Run all tests
python -m pytest tests/

# Run specific test file
python -m pytest tests/fr_proc_tests.py

# Run with verbose output
python -m pytest tests/ -v

# Run with coverage report
python -m pytest tests/ --cov=ASFINT
```

#### Creating New Unit Tests

Example test structure for FR Processor:
```python
import pytest
import pandas as pd
from ASFINT.Transform.FR_Processor import FR_ProcessorV2, FR_Helper

class TestFRProcessor:
    def test_fr_helper_basic(self):
        """Test FR_Helper with basic alphabet filtering"""
        # Create test DataFrame
        df = pd.DataFrame({
            'A': ['A', 'B', 'C', 'D', None],
            'B': [1, 2, 3, 4, 5]
        })
        
        # Test with alphabet A-C
        result = FR_Helper(df, alphabet=['A', 'B', 'C'])
        
        # Assertions
        assert len(result) == 3
        assert result.iloc[0, 0] == 'A'
        assert result.iloc[2, 0] == 'C'
    
    def test_fr_processor_date_extraction(self):
        """Test date extraction from text"""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        text_with_date = "Report dated 04/12/2024 shows..."
        
        result_df, extracted_date = FR_ProcessorV2(df, text_with_date)
        
        assert extracted_date == "04/12/2024"
        assert isinstance(result_df, pd.DataFrame)
    
    def test_fr_processor_no_date(self):
        """Test handling when no date is found"""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        text_no_date = "Report with no date information"
        
        result_df, extracted_date = FR_ProcessorV2(df, text_no_date)
        
        assert extracted_date == "00/00/0000"
```

### 2. Integration Testing

#### End-to-End Pipeline Testing
```python
import pytest
from ASFINT.Pipeline.workflow import run
from pathlib import Path
import tempfile
import shutil

class TestETLPipeline:
    def test_complete_fr_pipeline(self):
        """Test complete FR processing pipeline"""
        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = Path(temp_dir) / "input"
            output_dir = Path(temp_dir) / "output"
            input_dir.mkdir()
            output_dir.mkdir()
            
            # Create test FR file
            test_file = input_dir / "test_fr.csv"
            test_data = pd.DataFrame({
                'Appx': ['Appx', 'A', 'B', 'C', None],
                'Amount': [100, 200, 300, 400, 500]
            })
            test_data.to_csv(test_file, index=False)
            
            # Run pipeline
            run(
                pull_path=str(input_dir),
                push_path=str(output_dir),
                process_type="fr"
            )
            
            # Verify output
            output_files = list(output_dir.glob("*.csv"))
            assert len(output_files) == 1
            assert "GF" in output_files[0].name  # Good File tag
```

### 3. Data Validation Testing

#### Input Data Validation
```python
def test_input_data_validation():
    """Test validation of input data formats"""
    # Test valid CSV
    valid_df = pd.DataFrame({'A': [1, 2, 3], 'B': ['x', 'y', 'z']})
    assert isinstance(valid_df, pd.DataFrame)
    assert len(valid_df) > 0
    
    # Test invalid data handling
    invalid_data = "not a dataframe"
    with pytest.raises(AssertionError):
        FR_ProcessorV2(invalid_data, "test text")
```

#### Output Data Validation
```python
def test_output_data_validation():
    """Test that output data meets quality standards"""
    # Process test data
    input_df = pd.DataFrame({
        'Appx': ['Appx', 'A', 'B', 'C'],
        'Amount': [100, 200, 300, 400]
    })
    
    result_df, date = FR_ProcessorV2(input_df, "Report 04/12/2024")
    
    # Validate output
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) > 0
    assert 'Appx' in result_df.columns
    assert date == "04/12/2024"
    
    # Check data integrity
    assert not result_df.empty
    assert result_df.isnull().sum().sum() == 0  # No null values in key columns
```

### 4. Configuration Testing

#### Process Type Validation
```python
def test_process_type_configuration():
    """Test that all process types are properly configured"""
    from ASFINT.Config.Config import PROCESS_TYPES
    
    required_process_types = ['ABSA', 'CONTINGENCY', 'OASIS', 'FR']
    
    for process_type in required_process_types:
        assert process_type in PROCESS_TYPES
        
        config = PROCESS_TYPES[process_type]
        assert 'pull' in config
        assert 'push' in config
        assert 'process' in config
        assert 'naming' in config
        
        # Test function retrieval
        pull_func = get_pFuncs(process_type, 'pull')
        push_func = get_pFuncs(process_type, 'push')
        process_func = get_pFuncs(process_type, 'process')
        
        assert callable(pull_func)
        assert callable(push_func)
        assert callable(process_func)
```

### 5. Error Handling Testing

#### File Not Found Testing
```python
def test_file_not_found_handling():
    """Test handling of missing files"""
    from ASFINT.Pipeline.workflow import pull
    
    # Test with non-existent file
    result = pull("nonexistent_file.csv", "fr")
    assert result is None
    
    # Test with non-existent directory
    result = pull("nonexistent_directory/", "fr")
    assert result is None
```

#### Malformed Data Testing
```python
def test_malformed_data_handling():
    """Test handling of malformed input data"""
    # Create malformed DataFrame
    malformed_df = pd.DataFrame({
        'col1': [1, 2, None, 'invalid'],
        'col2': ['a', None, 3, 'b']
    })
    
    # Test that system handles gracefully
    try:
        result = FR_Helper(malformed_df, alphabet=['A', 'B'])
        # Should either process or raise informative error
    except Exception as e:
        assert "alphabet" in str(e).lower() or "data" in str(e).lower()
```

### 6. Performance Testing

#### Large Dataset Testing
```python
def test_large_dataset_performance():
    """Test performance with large datasets"""
    import time
    
    # Create large test dataset
    large_df = pd.DataFrame({
        'Appx': ['Appx'] + ['A'] * 10000 + [None],
        'Amount': range(10002)
    })
    
    start_time = time.time()
    result_df, date = FR_ProcessorV2(large_df, "Report 04/12/2024")
    end_time = time.time()
    
    # Performance assertions
    processing_time = end_time - start_time
    assert processing_time < 10  # Should process in under 10 seconds
    assert len(result_df) == 10000  # Correct number of rows processed
```

#### Memory Usage Testing
```python
def test_memory_usage():
    """Test memory efficiency"""
    import psutil
    import os
    
    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss
    
    # Process multiple large files
    for i in range(10):
        large_df = pd.DataFrame({
            'Appx': ['Appx'] + ['A'] * 1000 + [None],
            'Amount': range(1002)
        })
        FR_ProcessorV2(large_df, f"Report {i}")
    
    final_memory = process.memory_info().rss
    memory_increase = final_memory - initial_memory
    
    # Memory should not increase excessively
    assert memory_increase < 100 * 1024 * 1024  # Less than 100MB increase
```

### 7. Regression Testing

#### Data Consistency Testing
```python
def test_data_consistency():
    """Test that same input produces same output"""
    test_df = pd.DataFrame({
        'Appx': ['Appx', 'A', 'B', 'C'],
        'Amount': [100, 200, 300, 400]
    })
    test_text = "Report 04/12/2024"
    
    # Run multiple times
    results = []
    for _ in range(5):
        result_df, date = FR_ProcessorV2(test_df, test_text)
        results.append((result_df, date))
    
    # All results should be identical
    first_result = results[0]
    for result in results[1:]:
        pd.testing.assert_frame_equal(result[0], first_result[0])
        assert result[1] == first_result[1]
```

### 8. Manual Testing Procedures

#### Test Data Preparation
1. **Create test datasets** with known characteristics:
   - Valid FR files with proper "Appx" headers
   - Files with various date formats
   - Files with missing or malformed data
   - Large files for performance testing

2. **Expected output validation**:
   - Verify correct date extraction
   - Check proper alphabet-based filtering
   - Validate file naming conventions
   - Ensure data integrity

#### Manual Test Cases

**Test Case 1: Basic FR Processing**
```
Input: FR file with "Appx" header and A-C sections
Expected: Cleaned data with only A-C rows, correct date extraction
Validation: Check output file name, content, and date format
```

**Test Case 2: Date Extraction**
```
Input: FR file with various date formats (MM/DD/YYYY, YYYY-MM-DD)
Expected: Consistent date format in output
Validation: Verify date parsing and formatting
```

**Test Case 3: Error Handling**
```
Input: Malformed FR file or missing "Appx" header
Expected: Graceful error handling with informative messages
Validation: Check error messages and system stability
```

### 9. Continuous Integration Testing

#### Automated Test Pipeline
```yaml
# Example GitHub Actions workflow
name: ASFINT Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: 3.11
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install pytest pytest-cov
    - name: Run tests
      run: |
        pytest tests/ --cov=ASFINT --cov-report=xml
    - name: Upload coverage
      uses: codecov/codecov-action@v1
```

### 10. Production Validation

#### Pre-deployment Testing
1. **Data Quality Checks**:
   - Verify output data completeness
   - Check for data type consistency
   - Validate business rules

2. **Performance Benchmarks**:
   - Measure processing time for typical datasets
   - Monitor memory usage
   - Test with production-scale data volumes

3. **Integration Testing**:
   - Test with actual OCFO database connections
   - Verify file system permissions
   - Test error recovery procedures

## Test Data Management

### Creating Test Datasets
```python
def create_test_fr_file():
    """Create a standardized test FR file"""
    test_data = pd.DataFrame({
        'Appx': ['Appx', 'A', 'B', 'C', 'D', None],
        'Amount': [0, 100, 200, 300, 400, 500],
        'Description': ['Header', 'Item A', 'Item B', 'Item C', 'Item D', 'Footer']
    })
    return test_data

def create_test_oasis_file():
    """Create a standardized test OASIS file"""
    test_data = pd.DataFrame({
        'Year': ['2023-2024'] * 5,
        'Year Rank': [2023] * 5,
        'Active': [1] * 5,
        'OASIS RSO Designation': ['Club'] * 5
    })
    return test_data
```

## Reporting and Monitoring

### Test Results Reporting
```python
def generate_test_report():
    """Generate comprehensive test report"""
    import json
    from datetime import datetime
    
    test_results = {
        'timestamp': datetime.now().isoformat(),
        'test_suite': 'ASFINT',
        'results': {
            'unit_tests': run_unit_tests(),
            'integration_tests': run_integration_tests(),
            'performance_tests': run_performance_tests(),
            'data_validation_tests': run_data_validation_tests()
        }
    }
    
    with open('test_report.json', 'w') as f:
        json.dump(test_results, f, indent=2)
    
    return test_results
```

## Conclusion

This testing guide provides comprehensive methods to validate ASFINT functionality. Regular execution of these tests ensures system reliability, data quality, and performance standards. The combination of automated and manual testing approaches provides confidence in the system's ability to process financial data accurately and efficiently.

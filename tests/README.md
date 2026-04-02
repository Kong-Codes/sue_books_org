# Pipeline Tests

This directory contains comprehensive pytest tests for the data pipeline.

## Test Structure

Tests are located in the `tests/` directory at the project root and are organized by module:

- `tests/test_utils.py` - Tests for utility functions (logging, database operations, alerts)
- `tests/test_tables.py` - Tests for data loading, cleaning, and transformation functions
- `tests/test_schema_validation.py` - Tests for schema validation and drift detection
- `tests/test_pipeline_dag.py` - Tests for Airflow DAG structure and configuration
- `tests/conftest.py` - Shared pytest fixtures and configuration

## Running Tests

### Run All Tests
```bash
pytest
```

### Run Specific Test File
```bash
pytest tests/test_utils.py
pytest tests/test_tables.py
pytest tests/test_schema_validation.py
pytest tests/test_pipeline_dag.py
```

### Run Specific Test Class
```bash
pytest tests/test_utils.py::TestGetLogger
pytest tests/test_tables.py::TestCleanUsersData
```

### Run Specific Test Function
```bash
pytest tests/test_utils.py::TestGetLogger::test_get_logger_creates_logger
pytest tests/test_tables.py::TestCleanUsersData::test_clean_users_data_validates_email
```

### Run Tests with Coverage
```bash
pytest --cov=dags --cov-report=html --cov-report=term-missing
```

### Run Tests with Verbose Output
```bash
pytest -v
```

### Run Tests in Parallel
```bash
pytest -n auto
```

## Test Coverage

### utils.py Functions Tested
- ✅ `get_logger` - Logger creation and configuration
- ✅ `send_alert` - Discord alert sending
- ✅ `log_call` - Function call logging decorator
- ✅ `check_and_create_db` - Database creation
- ✅ `get_db` - Database connection
- ✅ `upsert_from_df` - DataFrame upsert operations
- ✅ `create_tables` - Table creation from SQL files
- ✅ Helper functions (`_truncate`, `_to_iso8601`)

### tables.py Functions Tested
- ✅ `load_data` - CSV data loading
- ✅ `clean_users_data` - User data cleaning and validation
- ✅ `clean_transactions_data` - Transaction data cleaning
- ✅ `clean_books_data` - Book data cleaning
- ✅ `daily_sales_table` - Daily sales aggregation
- ✅ `top_books` - Top books ranking
- ✅ `load_tables` - Table path loading
- ✅ `transform_data` - Complete data transformation pipeline
- ✅ `create_dim_tables` - Dimension table creation
- ✅ `create_fact_table` - Fact table creation

### schema_validation.py Functions Tested
- ✅ `_polars_dtype_to_str` - Data type conversion
- ✅ `infer_schema` - Schema inference from DataFrame
- ✅ `_index_by_name` - Column indexing
- ✅ `compare_schemas` - Schema comparison and drift detection
- ✅ `_ensure_dir` - Directory creation
- ✅ `_write_json` - JSON file writing
- ✅ `_read_json` - JSON file reading
- ✅ `validate_and_report` - Complete validation workflow

### pipeline_dag.py Functions Tested
- ✅ `on_task_success` - Success callback
- ✅ `on_task_failure` - Failure callback with alerting
- ✅ `default_args` - DAG configuration
- ✅ DAG structure and task dependencies
- ✅ Task callbacks configuration

## Test Fixtures

The `conftest.py` file provides reusable fixtures:

- `sample_users_df` - Sample users DataFrame
- `sample_transactions_df` - Sample transactions DataFrame
- `sample_books_df` - Sample books DataFrame
- `sample_csv_file` - Sample CSV file
- `temp_database_url` - Temporary database URL
- `mock_database_connection` - Mock database connection
- `test_log_file` - Test log file path
- `temp_schema_dir` - Temporary schema directory
- `sample_dim_user` - Sample dimension user table
- `sample_dim_book` - Sample dimension book table
- `sample_dim_date` - Sample dimension date table
- `sample_fact_transactions` - Sample fact transactions table
- `setup_test_env` - Environment setup (autouse)

## Writing New Tests

### Test Naming Convention
- Test files: `test_<module_name>.py`
- Test classes: `Test<ClassName>`
- Test functions: `test_<function_name>_<scenario>`

### Example Test Structure
```python
import pytest
from unittest.mock import Mock, patch
from module import function_to_test

class TestFunctionToTest:
    """Test function_to_test function"""
    
    def test_function_basic_case(self):
        """Test basic functionality"""
        result = function_to_test(input_data)
        assert result == expected_output
    
    def test_function_edge_case(self):
        """Test edge case handling"""
        result = function_to_test(edge_case_input)
        assert result is not None
    
    @patch('module.dependency')
    def test_function_with_mock(self, mock_dependency):
        """Test with mocked dependency"""
        mock_dependency.return_value = "mocked_value"
        result = function_to_test(input_data)
        assert result == expected_output
```

## Continuous Integration

Tests are designed to run in CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run Tests
  run: |
    pytest --cov=dags --cov-report=xml --cov-report=term
    
- name: Upload Coverage
  uses: codecov/codecov-action@v3
  with:
    file: ./coverage.xml
```

## Mocking External Dependencies

Tests use mocking for external dependencies:

- **Database connections** - Mocked with `unittest.mock.Mock`
- **File I/O** - Using `tmp_path` fixture for temporary files
- **Network requests** - Mocked with `unittest.mock.patch`
- **Environment variables** - Set with `monkeypatch` fixture

## Best Practices

1. **Isolation** - Each test should be independent
2. **Clarity** - Test names should clearly describe what they test
3. **Coverage** - Aim for high code coverage (>80%)
4. **Speed** - Tests should run quickly (use mocking)
5. **Maintainability** - Keep tests simple and readable

## Troubleshooting

### Import Errors
If you get import errors, ensure you're running tests from the project root:
```bash
cd /Users/mac/PycharmProjects/sales_platform/sales_project
pytest
```

### Database Connection Errors
Tests use mocked database connections. If you see real connection errors, check that mocks are properly configured.

### File Not Found Errors
Tests use temporary files via pytest fixtures. Ensure you're using `tmp_path` or `tmpdir` fixtures.

## Contributing

When adding new functionality:

1. Write tests first (TDD approach)
2. Ensure all tests pass
3. Maintain or improve code coverage
4. Update this README with new test information


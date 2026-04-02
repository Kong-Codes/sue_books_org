"""
Tests for schema_validation.py module
"""
import os
import pytest
import polars as pl
import json
import tempfile
from ..dags.schema_validation import (
    _polars_dtype_to_str,
    infer_schema,
    _index_by_name,
    compare_schemas,
    _ensure_dir,
    _write_json,
    _read_json,
    validate_and_report
)


class TestPolarsDtypeToStr:
    """Test _polars_dtype_to_str function"""
    
    def test_polars_dtype_to_str_int64(self):
        """Test dtype conversion for Int64"""
        dtype = pl.Int64
        result = _polars_dtype_to_str(dtype)
        assert isinstance(result, str)
    
    def test_polars_dtype_to_str_utf8(self):
        """Test dtype conversion for Utf8"""
        dtype = pl.Utf8
        result = _polars_dtype_to_str(dtype)
        assert isinstance(result, str)
    
    def test_polars_dtype_to_str_float64(self):
        """Test dtype conversion for Float64"""
        dtype = pl.Float64
        result = _polars_dtype_to_str(dtype)
        assert isinstance(result, str)
    
    def test_polars_dtype_to_str_date(self):
        """Test dtype conversion for Date"""
        dtype = pl.Date
        result = _polars_dtype_to_str(dtype)
        assert isinstance(result, str)


class TestInferSchema:
    """Test infer_schema function"""
    
    def test_infer_schema_basic(self):
        """Test schema inference for basic DataFrame"""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35]
        })
        
        result = infer_schema(df)
        
        assert isinstance(result, list)
        assert len(result) == 3
        assert all(isinstance(col, dict) for col in result)
    
    def test_infer_schema_with_nulls(self):
        """Test schema inference with nullable columns"""
        df = pl.DataFrame({
            "id": [1, 2, None],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, None, 35]
        })
        
        result = infer_schema(df)
        
        # Find nullable columns
        nullable_cols = [col for col in result if col["nullable"]]
        assert len(nullable_cols) >= 1
    
    def test_infer_schema_sorted_by_name(self):
        """Test that schema is sorted by column name"""
        df = pl.DataFrame({
            "zebra": [1, 2],
            "apple": ["A", "B"],
            "banana": [1.0, 2.0]
        })
        
        result = infer_schema(df)
        
        names = [col["name"] for col in result]
        assert names == sorted(names)
    
    def test_infer_schema_includes_type(self):
        """Test that schema includes type information"""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "price": [10.5, 20.0, 30.75]
        })
        
        result = infer_schema(df)
        
        for col in result:
            assert "type" in col
            assert "name" in col
            assert "nullable" in col
    
    def test_infer_schema_empty_dataframe(self):
        """Test schema inference for empty DataFrame"""
        df = pl.DataFrame({
            "id": [],
            "name": []
        })
        
        result = infer_schema(df)
        
        assert isinstance(result, list)
        assert len(result) == 2


class TestIndexByName:
    """Test _index_by_name function"""
    
    def test_index_by_name_creates_dict(self):
        """Test that function creates a dictionary indexed by name"""
        cols = [
            {"name": "id", "type": "Int64", "nullable": False},
            {"name": "name", "type": "Utf8", "nullable": False},
            {"name": "age", "type": "Int64", "nullable": True}
        ]
        
        result = _index_by_name(cols)
        
        assert isinstance(result, dict)
        assert "id" in result
        assert "name" in result
        assert "age" in result
        assert result["id"]["type"] == "Int64"
    
    def test_index_by_name_empty_list(self):
        """Test indexing with empty list"""
        result = _index_by_name([])
        assert isinstance(result, dict)
        assert len(result) == 0


class TestCompareSchemas:
    """Test compare_schemas function"""
    
    def test_compare_schemas_no_drift(self):
        """Test schema comparison with no drift"""
        baseline = [
            {"name": "id", "type": "Int64", "nullable": False},
            {"name": "name", "type": "Utf8", "nullable": False}
        ]
        current = [
            {"name": "id", "type": "Int64", "nullable": False},
            {"name": "name", "type": "Utf8", "nullable": False}
        ]
        
        result = compare_schemas(baseline, current)
        
        assert result["drift_detected"] is False
        assert len(result["added_columns"]) == 0
        assert len(result["removed_columns"]) == 0
        assert len(result["type_changes"]) == 0
        assert len(result["nullability_changes"]) == 0
    
    def test_compare_schemas_added_column(self):
        """Test schema comparison with added column"""
        baseline = [
            {"name": "id", "type": "Int64", "nullable": False},
            {"name": "name", "type": "Utf8", "nullable": False}
        ]
        current = [
            {"name": "id", "type": "Int64", "nullable": False},
            {"name": "name", "type": "Utf8", "nullable": False},
            {"name": "age", "type": "Int64", "nullable": False}
        ]
        
        result = compare_schemas(baseline, current)
        
        assert result["drift_detected"] is True
        assert "age" in result["added_columns"]
        assert len(result["removed_columns"]) == 0
    
    def test_compare_schemas_removed_column(self):
        """Test schema comparison with removed column"""
        baseline = [
            {"name": "id", "type": "Int64", "nullable": False},
            {"name": "name", "type": "Utf8", "nullable": False},
            {"name": "age", "type": "Int64", "nullable": False}
        ]
        current = [
            {"name": "id", "type": "Int64", "nullable": False},
            {"name": "name", "type": "Utf8", "nullable": False}
        ]
        
        result = compare_schemas(baseline, current)
        
        assert result["drift_detected"] is True
        assert "age" in result["removed_columns"]
        assert len(result["added_columns"]) == 0
    
    def test_compare_schemas_type_change(self):
        """Test schema comparison with type change"""
        baseline = [
            {"name": "id", "type": "Int64", "nullable": False},
            {"name": "price", "type": "Int64", "nullable": False}
        ]
        current = [
            {"name": "id", "type": "Int64", "nullable": False},
            {"name": "price", "type": "Float64", "nullable": False}
        ]
        
        result = compare_schemas(baseline, current)
        
        assert result["drift_detected"] is True
        assert len(result["type_changes"]) == 1
        assert result["type_changes"][0]["column"] == "price"
        assert result["type_changes"][0]["from"] == "Int64"
        assert result["type_changes"][0]["to"] == "Float64"
    
    def test_compare_schemas_nullability_change(self):
        """Test schema comparison with nullability change"""
        baseline = [
            {"name": "id", "type": "Int64", "nullable": False},
            {"name": "name", "type": "Utf8", "nullable": False}
        ]
        current = [
            {"name": "id", "type": "Int64", "nullable": False},
            {"name": "name", "type": "Utf8", "nullable": True}
        ]
        
        result = compare_schemas(baseline, current)
        
        assert result["drift_detected"] is True
        assert len(result["nullability_changes"]) == 1
        assert result["nullability_changes"][0]["column"] == "name"
    
    def test_compare_schemas_multiple_changes(self):
        """Test schema comparison with multiple types of changes"""
        baseline = [
            {"name": "id", "type": "Int64", "nullable": False},
            {"name": "old_col", "type": "Utf8", "nullable": False}
        ]
        current = [
            {"name": "id", "type": "Float64", "nullable": True},
            {"name": "new_col", "type": "Utf8", "nullable": False}
        ]
        
        result = compare_schemas(baseline, current)
        
        assert result["drift_detected"] is True
        assert "new_col" in result["added_columns"]
        assert "old_col" in result["removed_columns"]
        assert len(result["type_changes"]) == 1
        assert len(result["nullability_changes"]) == 1


class TestEnsureDir:
    """Test _ensure_dir function"""
    
    def test_ensure_dir_creates_directory(self, tmp_path):
        """Test that function creates directory if it doesn't exist"""
        new_dir = tmp_path / "new_directory"
        assert not new_dir.exists()
        
        _ensure_dir(str(new_dir))
        
        assert new_dir.exists()
        assert new_dir.is_dir()
    
    def test_ensure_dir_existing_directory(self, tmp_path):
        """Test that function doesn't fail if directory already exists"""
        existing_dir = tmp_path / "existing"
        existing_dir.mkdir()
        
        # Should not raise an error
        _ensure_dir(str(existing_dir))
        
        assert existing_dir.exists()


class TestWriteJson:
    """Test _write_json function"""
    
    def test_write_json_creates_file(self, tmp_path):
        """Test that function creates JSON file"""
        json_path = tmp_path / "test.json"
        data = {"key": "value", "number": 42}
        
        _write_json(str(json_path), data)
        
        assert json_path.exists()
    
    def test_write_json_correct_content(self, tmp_path):
        """Test that function writes correct content"""
        json_path = tmp_path / "test.json"
        data = {"key": "value", "number": 42}
        
        _write_json(str(json_path), data)
        
        with open(json_path, 'r') as f:
            loaded_data = json.load(f)
        
        assert loaded_data == data
    
    def test_write_json_with_indentation(self, tmp_path):
        """Test that function writes formatted JSON"""
        json_path = tmp_path / "test.json"
        data = {"key": "value"}
        
        _write_json(str(json_path), data)
        
        with open(json_path, 'r') as f:
            content = f.read()
        
        # Should contain newlines due to indentation
        assert '\n' in content


class TestReadJson:
    """Test _read_json function"""
    
    def test_read_json_loads_data(self, tmp_path):
        """Test that function loads JSON data"""
        json_path = tmp_path / "test.json"
        data = {"key": "value", "number": 42}
        
        with open(json_path, 'w') as f:
            json.dump(data, f)
        
        result = _read_json(str(json_path))
        
        assert result == data
    
    def test_read_json_nonexistent_file(self, tmp_path):
        """Test that function raises error for nonexistent file"""
        json_path = tmp_path / "nonexistent.json"
        
        with pytest.raises(FileNotFoundError):
            _read_json(str(json_path))


class TestValidateAndReport:
    """Test validate_and_report function"""
    
    def test_validate_and_report_creates_baseline(self, tmp_path):
        """Test that function creates baseline for new dataset"""
        baselines_dir = tmp_path / "baselines"
        reports_dir = tmp_path / "reports"
        
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })
        
        datasets = {"test_data": df}
        
        result = validate_and_report(datasets, str(tmp_path))
        
        assert result["any_drift"] is False
        assert "test_data" in result["datasets"]
        assert result["datasets"]["test_data"]["status"] == "baseline_created"
        
        # Check that baseline file was created
        baseline_path = baselines_dir / "test_data.schema.json"
        assert baseline_path.exists()
    
    def test_validate_and_report_no_drift(self, tmp_path):
        """Test validation with no drift"""
        baselines_dir = tmp_path / "baselines"
        reports_dir = tmp_path / "reports"
        baselines_dir.mkdir()
        reports_dir.mkdir()
        
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })
        
        # Create baseline
        baseline_schema = infer_schema(df)
        baseline_path = baselines_dir / "test_data.schema.json"
        _write_json(str(baseline_path), baseline_schema)
        
        datasets = {"test_data": df}
        
        result = validate_and_report(datasets, str(tmp_path))
        
        assert result["any_drift"] is False
        assert result["datasets"]["test_data"]["status"] == "no_drift"
    
    def test_validate_and_report_drift_detected(self, tmp_path):
        """Test validation with drift detected"""
        baselines_dir = tmp_path / "baselines"
        reports_dir = tmp_path / "reports"
        baselines_dir.mkdir()
        reports_dir.mkdir()
        
        baseline_df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })
        
        # Create baseline
        baseline_schema = infer_schema(baseline_df)
        baseline_path = baselines_dir / "test_data.schema.json"
        _write_json(str(baseline_path), baseline_schema)
        
        # Current data has different schema
        current_df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35]  # New column
        })
        
        datasets = {"test_data": current_df}
        
        result = validate_and_report(datasets, str(tmp_path))
        
        assert result["any_drift"] is True
        assert result["datasets"]["test_data"]["status"] == "drift_detected"
        assert result["datasets"]["test_data"]["drift"]["drift_detected"] is True
        assert "age" in result["datasets"]["test_data"]["drift"]["added_columns"]
    
    def test_validate_and_report_multiple_datasets(self, tmp_path):
        """Test validation with multiple datasets"""
        baselines_dir = tmp_path / "baselines"
        reports_dir = tmp_path / "reports"
        
        df1 = pl.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        df2 = pl.DataFrame({"product_id": [101, 102], "price": [10.0, 20.0]})
        
        datasets = {
            "users": df1,
            "products": df2
        }
        
        result = validate_and_report(datasets, str(tmp_path))
        
        assert len(result["datasets"]) == 2
        assert "users" in result["datasets"]
        assert "products" in result["datasets"]
    
    def test_validate_and_report_creates_reports(self, tmp_path):
        """Test that function creates report files"""
        baselines_dir = tmp_path / "baselines"
        reports_dir = tmp_path / "reports"
        
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })
        
        datasets = {"test_data": df}
        
        validate_and_report(datasets, str(tmp_path))
        
        # Check that report file was created
        report_path = reports_dir / "test_data.report.json"
        assert report_path.exists()
    
    def test_validate_and_report_sends_alert_on_drift(self, mocker, tmp_path):
        """Test that function sends alert when drift is detected"""
        mock_send_alert = mocker.patch('sales_project.dags.schema_validation.send_alert')
        
        baselines_dir = tmp_path / "baselines"
        reports_dir = tmp_path / "reports"
        baselines_dir.mkdir()
        reports_dir.mkdir()
        
        baseline_df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })
        
        # Create baseline
        baseline_schema = infer_schema(baseline_df)
        baseline_path = baselines_dir / "test_data.schema.json"
        _write_json(str(baseline_path), baseline_schema)
        
        # Current data has different schema
        current_df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "new_col": [1, 2, 3]  # New column
        })
        
        datasets = {"test_data": current_df}
        
        validate_and_report(datasets, str(tmp_path))
        
        # Verify alert was sent
        mock_send_alert.assert_called_once()
        call_args = mock_send_alert.call_args
        assert "Schema Drift Detected" in call_args[0][0]
        assert call_args[1]["level"] == "warn"
    
    def test_validate_and_report_no_alert_on_no_drift(self, mocker, tmp_path):
        """Test that function doesn't send alert when there's no drift"""
        mock_send_alert = mocker.patch('sales_project.dags.schema_validation.send_alert')
        
        baselines_dir = tmp_path / "baselines"
        reports_dir = tmp_path / "reports"
        baselines_dir.mkdir()
        reports_dir.mkdir()
        
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })
        
        # Create baseline
        baseline_schema = infer_schema(df)
        baseline_path = baselines_dir / "test_data.schema.json"
        _write_json(str(baseline_path), baseline_schema)
        
        datasets = {"test_data": df}
        
        validate_and_report(datasets, str(tmp_path))
        
        # Verify alert was not sent
        mock_send_alert.assert_not_called()


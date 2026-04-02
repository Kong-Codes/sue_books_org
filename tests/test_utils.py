"""
Tests for utils.py module
"""
import os
import pytest
import tempfile
import polars as pl
import requests
from ..dags.utils import (
    get_logger,
    send_alert,
    log_call,
    check_and_create_db,
    get_db,
    upsert_from_df,
    create_tables,
    _truncate,
    _to_iso8601
)


class TestGetLogger:
    """Test get_logger function"""
    
    def test_get_logger_creates_logger(self):
        """Test that get_logger creates a logger instance"""
        logger = get_logger("test_logger")
        assert logger is not None
        assert logger.name == "test_logger"
    
    def test_get_logger_with_custom_log_file(self):
        """Test get_logger with custom log file path"""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, "test.log")
            logger = get_logger("test_logger_custom_file", log_file)
            assert logger is not None
            assert os.path.exists(log_file)
    
    def test_get_logger_prevents_duplicate_handlers(self):
        """Test that get_logger doesn't add duplicate handlers"""
        logger = get_logger("test_logger")
        initial_handler_count = len(logger.handlers)
        
        # Call again with same name
        logger2 = get_logger("test_logger")
        assert len(logger2.handlers) == initial_handler_count
    
    def test_get_logger_logs_to_console_and_file(self):
        """Test that logger outputs to both console and file"""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, "test.log")
            logger = get_logger("test_logger_console_file", log_file)
            
            logger.info("Test message")
            
            # Check file was created
            assert os.path.exists(log_file)
            
            # Check log content
            with open(log_file, 'r') as f:
                content = f.read()
                assert "Test message" in content


class TestTruncate:
    """Test _truncate helper function"""
    
    def test_truncate_within_limit(self):
        """Test truncate when string is within limit"""
        result = _truncate("hello", 10)
        assert result == "hello"
    
    def test_truncate_exceeds_limit(self):
        """Test truncate when string exceeds limit"""
        result = _truncate("hello world", 5)
        assert result == "hell…"
    
    def test_truncate_exactly_at_limit(self):
        """Test truncate when string is exactly at limit"""
        result = _truncate("hello", 5)
        assert result == "hello"


class TestToIso8601:
    """Test _to_iso8601 helper function"""
    
    def test_to_iso8601_converts_timestamp(self):
        """Test that timestamp is converted to ISO8601 format"""
        ts_ms = 1609459200000  # 2021-01-01 00:00:00 UTC
        result = _to_iso8601(ts_ms)
        assert "2021-01-01T00:00:00" in result
        assert result.endswith("+00:00")


class TestSendAlert:
    """Test send_alert function"""
    
    def test_send_alert_success(self, mocker):
        """Test successful alert sending"""
        mocker.patch.dict(os.environ, {'ALERT_WEBHOOK_URL': 'https://discord.com/api/webhooks/test'})
        mock_post = mocker.patch('sales_project.dags.utils.requests.post')
        
        mock_response = mocker.Mock()
        mock_response.status_code = 204
        mock_post.return_value = mock_response
        
        result = send_alert("Test Alert", {"key": "value"})
        assert result is True
        mock_post.assert_called_once()
    
    def test_send_alert_no_webhook_url(self, mocker):
        """Test alert when webhook URL is not set"""
        mocker.patch.dict(os.environ, {'ALERT_WEBHOOK_URL': ''})
        
        result = send_alert("Test Alert", {"key": "value"})
        assert result is False
    
    def test_send_alert_rate_limited(self, mocker):
        """Test alert when rate limited by Discord"""
        mocker.patch.dict(os.environ, {'ALERT_WEBHOOK_URL': 'https://discord.com/api/webhooks/test'})
        mock_post = mocker.patch('sales_project.dags.utils.requests.post')
        
        mock_response = mocker.Mock()
        mock_response.status_code = 429
        mock_response.json.return_value = {"retry_after": 2}
        mock_post.return_value = mock_response
        
        result = send_alert("Test Alert", {"key": "value"})
        assert result is False
    
    def test_send_alert_request_exception(self, mocker):
        """Test alert when request fails"""
        mocker.patch.dict(os.environ, {'ALERT_WEBHOOK_URL': 'https://discord.com/api/webhooks/test'})
        mock_post = mocker.patch('sales_project.dags.utils.requests.post')
        
        mock_post.side_effect = requests.RequestException("Connection error")
        
        result = send_alert("Test Alert", {"key": "value"})
        assert result is False
    
    def test_send_alert_with_different_levels(self, mocker):
        """Test alert with different severity levels"""
        mocker.patch.dict(os.environ, {'ALERT_WEBHOOK_URL': 'https://discord.com/api/webhooks/test'})
        mock_post = mocker.patch('sales_project.dags.utils.requests.post')
        
        mock_response = mocker.Mock()
        mock_response.status_code = 204
        mock_post.return_value = mock_response
        
        send_alert("Info Alert", {}, level="info")
        send_alert("Warning Alert", {}, level="warn")
        send_alert("Error Alert", {}, level="error")
        
        assert mock_post.call_count == 3


class TestLogCall:
    """Test log_call decorator"""
    
    def test_log_call_decorator_success(self, mocker):
        """Test decorator logs successful function execution"""
        logger = mocker.Mock()
        
        @log_call(logger, "test_operation")
        def test_func():
            return "success"
        
        result = test_func()
        assert result == "success"
        logger.info.assert_called()
        assert "test_operation completed successfully" in logger.info.call_args[0][0]
    
    def test_log_call_decorator_failure(self, mocker):
        """Test decorator logs failed function execution"""
        logger = mocker.Mock()
        
        @log_call(logger, "test_operation")
        def test_func():
            raise ValueError("Test error")
        
        with pytest.raises(ValueError):
            test_func()
        
        logger.error.assert_called()
        assert "test_operation failed" in logger.error.call_args[0][0]


class TestCheckAndCreateDb:
    """Test check_and_create_db function"""
    
    def test_check_and_create_db_creates_new_db(self, mocker):
        """Test that function creates database if it doesn't exist"""
        mock_connect = mocker.patch('sales_project.dags.utils.psycopg2.connect')
        mock_conn = mocker.Mock()
        mock_cur = mocker.Mock()
        mock_cur.fetchone.return_value = None  # DB doesn't exist
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value = mock_conn
        
        check_and_create_db("test_db", "postgresql://localhost/postgres")
        
        mock_cur.execute.assert_called()
        mock_conn.close.assert_called()
    
    def test_check_and_create_db_db_exists(self, mocker):
        """Test that function doesn't create database if it exists"""
        mock_connect = mocker.patch('sales_project.dags.utils.psycopg2.connect')
        mock_conn = mocker.Mock()
        mock_cur = mocker.Mock()
        mock_cur.fetchone.return_value = (1,)  # DB exists
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value = mock_conn
        
        check_and_create_db("test_db", "postgresql://localhost/postgres")
        
        # Should not call CREATE DATABASE
        mock_cur.execute.assert_called()
        mock_conn.close.assert_called()
    
    def test_check_and_create_db_no_url(self):
        """Test that function raises error when URL is not provided"""
        with pytest.raises(ValueError):
            check_and_create_db("test_db", None)


class TestGetDb:
    """Test get_db function"""
    
    def test_get_db_success(self, mocker):
        """Test successful database connection"""
        mock_connect = mocker.patch('sales_project.dags.utils.psycopg2.connect')
        mock_conn = mocker.Mock()
        mock_connect.return_value = mock_conn
        
        result = get_db("postgresql://localhost/test_db")
        assert result == mock_conn
        mock_connect.assert_called_once_with("postgresql://localhost/test_db")
    
    def test_get_db_connection_error(self, mocker):
        """Test database connection error handling"""
        mock_connect = mocker.patch('sales_project.dags.utils.psycopg2.connect')
        mock_connect.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception):
            get_db("postgresql://localhost/test_db")
    
    def test_get_db_no_url(self):
        """Test that function raises error when URL is not provided"""
        with pytest.raises(ValueError):
            get_db(None)


class TestUpsertFromDf:
    """Test upsert_from_df function"""
    
    def test_upsert_from_df_empty_dataframe(self, mocker):
        """Test upsert with empty DataFrame"""
        mock_conn = mocker.Mock()
        df = pl.DataFrame({"id": [], "name": []})
        
        upsert_from_df(mock_conn, df, "test_table", ["id"])
        
        # Should not execute any SQL
        mock_conn.cursor.assert_not_called()
    
    def test_upsert_from_df_missing_conflict_columns(self, mocker):
        """Test upsert with missing conflict columns"""
        mock_conn = mocker.Mock()
        df = pl.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        
        with pytest.raises(ValueError):
            upsert_from_df(mock_conn, df, "test_table", ["missing_column"])
    
    def test_upsert_from_df_duplicates(self, mocker):
        """Test upsert handles duplicate rows"""
        mocker.patch('sales_project.dags.utils.execute_values')
        mock_conn = mocker.MagicMock()
        mock_cur = mocker.MagicMock()
        mock_conn.cursor.return_value = mock_cur
        
        df = pl.DataFrame({"id": [1, 1, 2], "name": ["A", "B", "C"]})
        
        upsert_from_df(mock_conn, df, "test_table", ["id"])
        
        # Should deduplicate and keep last
        assert mock_conn.commit.called
    
    def test_upsert_from_df_with_update_columns(self, mocker):
        """Test upsert with specific update columns"""
        mocker.patch('sales_project.dags.utils.execute_values')
        mock_conn = mocker.MagicMock()
        mock_cur = mocker.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        
        df = pl.DataFrame({"id": [1, 2], "name": ["A", "B"], "age": [25, 30]})
        
        upsert_from_df(mock_conn, df, "test_table", ["id"], update_columns=["name"])
        
        mock_conn.commit.assert_called()
    
    def test_upsert_from_df_chunking(self, mocker):
        """Test upsert with chunking for large datasets"""
        mock_execute_values = mocker.patch('sales_project.dags.utils.execute_values')
        mock_conn = mocker.MagicMock()
        mock_cur = mocker.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        
        # Create large DataFrame
        df = pl.DataFrame({"id": range(25000), "name": ["A"] * 25000})
        
        upsert_from_df(mock_conn, df, "test_table", ["id"], chunk_size=10000)
        
        # Should be called multiple times due to chunking
        assert mock_execute_values.call_count >= 2
        mock_conn.commit.assert_called()
    
    def test_upsert_from_df_rollback_on_error(self, mocker):
        """Test that upsert rolls back on error"""
        mock_conn = mocker.MagicMock()
        mock_cur = mocker.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_conn.cursor.return_value.__enter__.side_effect = Exception("SQL Error")
        
        df = pl.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        
        with pytest.raises(Exception):
            upsert_from_df(mock_conn, df, "test_table", ["id"])
        
        mock_conn.rollback.assert_called()


class TestCreateTables:
    """Test create_tables function"""
    
    def test_create_tables_success(self, mocker):
        """Test successful table creation"""
        mock_open = mocker.patch('builtins.open', create=True)
        mock_check_db = mocker.patch('sales_project.dags.utils.check_and_create_db')
        mock_get_db = mocker.patch('sales_project.dags.utils.get_db')
        
        mock_conn = mocker.Mock()
        mock_cur = mocker.Mock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchall.return_value = [("olap", "table1", "dim_user")]
        mock_get_db.return_value = mock_conn
        
        # Mock file reading
        mock_file = mocker.Mock()
        mock_file.read.return_value = "CREATE TABLE test;"
        mock_open.return_value.__enter__.return_value = mock_file
        
        create_tables("test_db", "postgresql://localhost/test_db")
        
        mock_check_db.assert_called_once()
        mock_conn.commit.assert_called()
        mock_conn.close.assert_called()
    
    def test_create_tables_error_handling(self, mocker):
        """Test table creation error handling"""
        mocker.patch('sales_project.dags.utils.check_and_create_db')
        mock_get_db = mocker.patch('sales_project.dags.utils.get_db')
        
        mock_conn = mocker.Mock()
        mock_cur = mocker.Mock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.execute.side_effect = Exception("SQL Error")
        mock_get_db.return_value = mock_conn
        
        create_tables("test_db", "postgresql://localhost/test_db")

        mock_conn.rollback.assert_called()


"""
Pytest configuration and shared fixtures
"""
import pytest
import polars as pl
import os
import tempfile
from pathlib import Path


@pytest.fixture
def sample_users_df():
    """Fixture providing sample users DataFrame"""
    return pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
        "signup_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "social_security_number": ["123-45-6789", "987-65-4321", "456-78-9012"],
        "location": ["NYC", "LA", "Chicago"]
    })


@pytest.fixture
def sample_transactions_df():
    """Fixture providing sample transactions DataFrame"""
    return pl.DataFrame({
        "transaction_id": [1, 2, 3, 4],
        "user_id": ["USER_1", "USER_2", "USER_1", "USER_3"],
        "book_id": [101, 102, 101, 103],
        "amount": [10.0, 20.0, 15.0, 25.0],
        "timestamp": [
            "2024-01-01 10:00:00",
            "2024-01-01 11:00:00",
            "2024-01-02 10:00:00",
            "2024-01-02 11:00:00"
        ]
    })


@pytest.fixture
def sample_books_df():
    """Fixture providing sample books DataFrame"""
    return pl.DataFrame({
        "book_id": [101, 102, 103],
        "title": ["Book A", "Book B", "Book C"],
        "author": ["Author A", "Author B", "Author C"],
        "category": ["Fiction", "Non-Fiction", "Fiction"],
        "base_price": [10.0, 15.0, 12.0]
    })


@pytest.fixture
def sample_csv_file(tmp_path):
    """Fixture providing a sample CSV file"""
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("id,name,age\n1,Alice,25\n2,Bob,30\n")
    return str(csv_path)


@pytest.fixture
def temp_database_url():
    """Fixture providing a temporary database URL for testing"""
    return "postgresql://test_user:test_pass@localhost:5432/test_db"


@pytest.fixture
def mock_database_connection(mocker):
    """Fixture providing a mock database connection"""
    mock_conn = mocker.Mock()
    mock_cur = mocker.Mock()
    mock_conn.cursor.return_value = mock_cur
    return mock_conn


@pytest.fixture
def test_log_file(tmp_path):
    """Fixture providing a test log file path"""
    log_file = tmp_path / "test.log"
    return str(log_file)


@pytest.fixture
def temp_schema_dir(tmp_path):
    """Fixture providing a temporary schema directory"""
    schema_dir = tmp_path / "schema"
    schema_dir.mkdir()
    baselines_dir = schema_dir / "baselines"
    reports_dir = schema_dir / "reports"
    baselines_dir.mkdir()
    reports_dir.mkdir()
    return str(schema_dir)


@pytest.fixture
def sample_dim_user():
    """Fixture providing sample dim_user DataFrame"""
    return pl.DataFrame({
        "user_sk": [1, 2, 3],
        "user_id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
        "location": ["NYC", "LA", "Chicago"],
        "signup_date": ["2024-01-01", "2024-01-02", "2024-01-03"]
    }).with_columns(pl.col("signup_date").str.strptime(pl.Date, "%Y-%m-%d"))


@pytest.fixture
def sample_dim_book():
    """Fixture providing sample dim_book DataFrame"""
    return pl.DataFrame({
        "book_sk": [1, 2, 3],
        "book_id": [101, 102, 103],
        "title": ["Book A", "Book B", "Book C"],
        "author": ["Author A", "Author B", "Author C"],
        "category": ["Fiction", "Non-Fiction", "Fiction"],
        "base_price": [10.0, 15.0, 12.0]
    })


@pytest.fixture
def sample_dim_date():
    """Fixture providing sample dim_date DataFrame"""
    return pl.DataFrame({
        "date_sk": [20240101, 20240102, 20240103],
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "year": [2024, 2024, 2024],
        "quarter": [1, 1, 1],
        "month": [1, 1, 1],
        "week": [1, 1, 1],
        "day_of_week": [1, 2, 3]
    }).with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))


@pytest.fixture
def sample_fact_transactions():
    """Fixture providing sample fact_transactions DataFrame"""
    return pl.DataFrame({
        "transaction_id": [1, 2, 3, 4],
        "user_sk": [1, 2, 1, 3],
        "book_sk": [1, 2, 1, 3],
        "date_sk": [20240101, 20240101, 20240102, 20240102],
        "amount": [10.0, 20.0, 15.0, 25.0]
    })


@pytest.fixture(autouse=True)
def setup_test_env(monkeypatch):
    """Fixture that sets up test environment variables"""
    # Set test environment variables
    monkeypatch.setenv("DATABASE_URL", "postgresql://test_user:test_pass@localhost:5432/test_db")
    monkeypatch.setenv("DATABASE_NAME", "test_db")
    monkeypatch.setenv("SQL_DIR", "/tmp/test_sql")
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://discord.com/api/webhooks/test")
    
    # Create temporary SQL directory
    os.makedirs("/tmp/test_sql", exist_ok=True)
    
    # Create mock SQL files
    with open("/tmp/test_sql/oltp_schema.sql", "w") as f:
        f.write("CREATE SCHEMA IF NOT EXISTS oltp;")
    
    with open("/tmp/test_sql/olap_schema.sql", "w") as f:
        f.write("CREATE SCHEMA IF NOT EXISTS olap;")
    
    yield
    
    # Cleanup
    import shutil
    if os.path.exists("/tmp/test_sql"):
        shutil.rmtree("/tmp/test_sql")


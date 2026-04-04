"""
Tests for tables.py module
"""
import polars as pl
from ..dags.tables import (
    load_data,
    clean_users_data,
    clean_transactions_data,
    clean_books_data,
    daily_sales_table,
    top_books,
    load_tables,
    transform_data,
    create_dim_tables,
    create_fact_table
)


class TestLoadData:
    """Test load_data function"""
    
    def test_load_data_from_csv(self, tmp_path):
        """Test loading data from CSV file"""
        # Create test CSV
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("id,name\n1,Alice\n2,Bob\n")
        
        df = load_data(str(csv_path))
        
        assert isinstance(df, pl.DataFrame)
        assert df.height == 2
        assert "id" in df.columns
        assert "name" in df.columns
    
    def test_load_data_empty_file(self, tmp_path):
        """Test loading empty CSV file"""
        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("id,name\n")
        
        df = load_data(str(csv_path))
        
        assert isinstance(df, pl.DataFrame)
        assert df.height == 0


class TestCleanUsersData:
    """Test clean_users_data function"""
    
    def test_clean_users_data_validates_email(self):
        """Test that invalid emails are filtered out"""
        users = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "email": ["alice@example.com", "invalid-email", "charlie@example.com"],
            "signup_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "social_security_number": ["123-45-6789", "987-65-4321", "456-78-9012"],
            "location": ["NYC", "LA", "Chicago"]
        })
        
        result = clean_users_data(users)
        
        assert result.height == 2
        assert "invalid-email" not in result["email"].to_list()
    
    def test_clean_users_data_fixes_invalid_date(self):
        """Test that invalid dates are corrected"""
        users = pl.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "email": ["alice@example.com", "bob@example.com"],
            "signup_date": ["2024-13-45", "2024-01-02"],
            "social_security_number": ["123-45-6789", "987-65-4321"],
            "location": ["NYC", "LA"]
        })
        
        result = clean_users_data(users)
        
        # Check that invalid date was replaced
        dates = result["signup_date"].to_list()
        assert "2024-13-45" not in [str(d) for d in dates]
    
    def test_clean_users_data_removes_ssn_dashes(self):
        """Test that SSN dashes are removed"""
        users = pl.DataFrame({
            "id": [1],
            "name": ["Alice"],
            "email": ["alice@example.com"],
            "signup_date": ["2024-01-01"],
            "social_security_number": ["123-45-6789"],
            "location": ["NYC"]
        })
        
        result = clean_users_data(users)
        
        ssn = result["social_security_number"].to_list()[0]
        assert "-" not in str(ssn)
        assert isinstance(ssn, int)
    
    def test_clean_users_data_replaces_invalid_ssn(self):
        """Test that invalid SSN is replaced with 0"""
        users = pl.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "email": ["alice@example.com", "bob@example.com"],
            "signup_date": ["2024-01-01", "2024-01-02"],
            "social_security_number": ["123-45-INVALID", "987-65-4321"],
            "location": ["NYC", "LA"]
        })
        
        result = clean_users_data(users)
        
        ssns = result["social_security_number"].to_list()
        assert 0 in ssns


class TestCleanTransactionsData:
    """Test clean_transactions_data function"""
    
    def test_clean_transactions_data_removes_user_prefix(self):
        """Test that USER_ prefix is removed from user_id"""
        transactions = pl.DataFrame({
            "transaction_id": [1, 2],
            "user_id": ["USER_1", "USER_2"],
            "book_id": [101, 102],
            "amount": [10.0, 20.0],
            "timestamp": ["2024-01-01 10:00:00", "2024-01-01 11:00:00"]
        })
        
        users = pl.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "email": ["alice@example.com", "bob@example.com"],
            "signup_date": ["2024-01-01", "2024-01-01"],
            "social_security_number": ["123456789", "987654321"],
            "location": ["NYC", "LA"]
        })
        
        result = clean_transactions_data(transactions, users)
        
        user_ids = result["user_id"].to_list()
        assert "USER_1" not in [str(u) for u in user_ids]
        assert 1 in user_ids
    
    def test_clean_transactions_data_filters_zero_amounts(self):
        """Test that zero amount transactions are filtered out"""
        transactions = pl.DataFrame({
            "transaction_id": [1, 2, 3],
            "user_id": ["USER_1", "USER_2", "USER_1"],
            "book_id": [101, 102, 103],
            "amount": [10.0, 0.0, 20.0],
            "timestamp": ["2024-01-01 10:00:00", "2024-01-01 11:00:00", "2024-01-01 12:00:00"]
        })
        
        users = pl.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "email": ["alice@example.com", "bob@example.com"],
            "signup_date": ["2024-01-01", "2024-01-01"],
            "social_security_number": ["123456789", "987654321"],
            "location": ["NYC", "LA"]
        })
        
        result = clean_transactions_data(transactions, users)
        
        assert result.height == 2
        assert 0.0 not in result["amount"].to_list()
    
    def test_clean_transactions_data_filters_negative_amounts(self):
        """Test that negative amount transactions are filtered out"""
        transactions = pl.DataFrame({
            "transaction_id": [1, 2],
            "user_id": ["USER_1", "USER_2"],
            "book_id": [101, 102],
            "amount": [10.0, -5.0],
            "timestamp": ["2024-01-01 10:00:00", "2024-01-01 11:00:00"]
        })
        
        users = pl.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "email": ["alice@example.com", "bob@example.com"],
            "signup_date": ["2024-01-01", "2024-01-01"],
            "social_security_number": ["123456789", "987654321"],
            "location": ["NYC", "LA"]
        })
        
        result = clean_transactions_data(transactions, users)
        
        assert result.height == 1
        assert -5.0 not in result["amount"].to_list()
    
    def test_clean_transactions_data_filters_zero_book_id(self):
        """Test that zero book_id transactions are filtered out"""
        transactions = pl.DataFrame({
            "transaction_id": [1, 2],
            "user_id": ["USER_1", "USER_2"],
            "book_id": [101, 0],
            "amount": [10.0, 20.0],
            "timestamp": ["2024-01-01 10:00:00", "2024-01-01 11:00:00"]
        })
        
        users = pl.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "email": ["alice@example.com", "bob@example.com"],
            "signup_date": ["2024-01-01", "2024-01-01"],
            "social_security_number": ["123456789", "987654321"],
            "location": ["NYC", "LA"]
        })
        
        result = clean_transactions_data(transactions, users)
        
        assert result.height == 1
        assert 0 not in result["book_id"].to_list()
    
    def test_clean_transactions_data_semi_join_with_users(self):
        """Test that only transactions with valid users are kept"""
        transactions = pl.DataFrame({
            "transaction_id": [1, 2, 3],
            "user_id": ["USER_1", "USER_2", "USER_999"],
            "book_id": [101, 102, 103],
            "amount": [10.0, 20.0, 30.0],
            "timestamp": ["2024-01-01 10:00:00", "2024-01-01 11:00:00", "2024-01-01 12:00:00"]
        })
        
        users = pl.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "email": ["alice@example.com", "bob@example.com"],
            "signup_date": ["2024-01-01", "2024-01-01"],
            "social_security_number": ["123456789", "987654321"],
            "location": ["NYC", "LA"]
        })
        
        result = clean_transactions_data(transactions, users)
        
        # Should only keep transactions for users 1 and 2
        assert result.height == 2
        assert 999 not in result["user_id"].to_list()


class TestCleanBooksData:
    """Test clean_books_data function"""
    
    def test_clean_books_data_returns_same(self):
        """Test that clean_books_data returns books as-is"""
        books = pl.DataFrame({
            "book_id": [1, 2, 3],
            "title": ["Book A", "Book B", "Book C"],
            "author": ["Author A", "Author B", "Author C"],
            "category": ["Fiction", "Non-Fiction", "Fiction"],
            "base_price": [10.0, 15.0, 12.0]
        })
        
        result = clean_books_data(books)
        
        assert result.equals(books)


class TestDailySalesTable:
    """Test daily_sales_table function"""
    
    def test_daily_sales_table_aggregates_by_date(self):
        """Test that daily sales aggregates transactions by date"""
        transactions = pl.DataFrame({
            "transaction_id": [1, 2, 3, 4],
            "user_id": [1, 1, 2, 2],
            "book_id": [101, 102, 101, 103],
            "amount": [10.0, 20.0, 15.0, 25.0],
            "timestamp": [
                "2024-01-01 10:00:00",
                "2024-01-01 11:00:00",
                "2024-01-02 10:00:00",
                "2024-01-02 11:00:00"
            ]
        }).with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
        
        result = daily_sales_table(transactions)
        
        assert result.height == 2
        assert "date_key" in result.columns
        assert "revenue" in result.columns
        assert "num_transactions" in result.columns
        assert "active_users" in result.columns
    
    def test_daily_sales_table_calculates_revenue(self):
        """Test that daily sales correctly calculates revenue"""
        transactions = pl.DataFrame({
            "transaction_id": [1, 2, 3],
            "user_id": [1, 1, 2],
            "book_id": [101, 102, 101],
            "amount": [10.0, 20.0, 15.0],
            "timestamp": [
                "2024-01-01 10:00:00",
                "2024-01-01 11:00:00",
                "2024-01-01 12:00:00"
            ]
        }).with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
        
        result = daily_sales_table(transactions)
        
        assert result.height == 1
        assert result["revenue"].sum() == 45.0
    
    def test_daily_sales_table_counts_transactions(self):
        """Test that daily sales counts transactions correctly"""
        transactions = pl.DataFrame({
            "transaction_id": [1, 2, 3],
            "user_id": [1, 1, 2],
            "book_id": [101, 102, 101],
            "amount": [10.0, 20.0, 15.0],
            "timestamp": [
                "2024-01-01 10:00:00",
                "2024-01-01 11:00:00",
                "2024-01-01 12:00:00"
            ]
        }).with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
        
        result = daily_sales_table(transactions)
        
        assert result["num_transactions"].sum() == 3
    
    def test_daily_sales_table_counts_active_users(self):
        """Test that daily sales counts unique users correctly"""
        transactions = pl.DataFrame({
            "transaction_id": [1, 2, 3],
            "user_id": [1, 1, 2],
            "book_id": [101, 102, 101],
            "amount": [10.0, 20.0, 15.0],
            "timestamp": [
                "2024-01-01 10:00:00",
                "2024-01-01 11:00:00",
                "2024-01-01 12:00:00"
            ]
        }).with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
        
        result = daily_sales_table(transactions)
        
        assert result["active_users"].sum() == 2


class TestTopBooks:
    """Test top_books function"""
    
    def test_top_books_aggregates_by_book(self):
        """Test that top_books aggregates by book_id"""
        transactions = pl.DataFrame({
            "transaction_id": [1, 2, 3, 4],
            "user_id": [1, 1, 2, 2],
            "book_id": [101, 101, 102, 102],
            "amount": [10.0, 15.0, 20.0, 25.0],
            "timestamp": [
                "2024-01-01 10:00:00",
                "2024-01-01 11:00:00",
                "2024-01-01 12:00:00",
                "2024-01-01 13:00:00"
            ]
        }).with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
        
        books = pl.DataFrame({
            "book_id": [101, 102, 103],
            "title": ["Book A", "Book B", "Book C"],
            "author": ["Author A", "Author B", "Author C"],
            "category": ["Fiction", "Non-Fiction", "Fiction"],
            "base_price": [10.0, 15.0, 12.0]
        })
        
        result = top_books(transactions, books)
        
        assert result.height == 2
        assert "revenue" in result.columns
        assert "num_sales" in result.columns
        assert "unique_buyers" in result.columns
    
    def test_top_books_calculates_revenue(self):
        """Test that top_books correctly calculates revenue per book"""
        transactions = pl.DataFrame({
            "transaction_id": [1, 2, 3],
            "user_id": [1, 1, 2],
            "book_id": [101, 101, 101],
            "amount": [10.0, 15.0, 20.0],
            "timestamp": [
                "2024-01-01 10:00:00",
                "2024-01-01 11:00:00",
                "2024-01-01 12:00:00"
            ]
        }).with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
        
        books = pl.DataFrame({
            "book_id": [101],
            "title": ["Book A"],
            "author": ["Author A"],
            "category": ["Fiction"],
            "base_price": [10.0]
        })
        
        result = top_books(transactions, books)
        
        assert result["revenue"].sum() == 45.0
    
    def test_top_books_sorts_by_revenue(self):
        """Test that top_books sorts by revenue descending"""
        transactions = pl.DataFrame({
            "transaction_id": [1, 2, 3],
            "user_id": [1, 2, 3],
            "book_id": [101, 102, 103],
            "amount": [10.0, 30.0, 20.0],
            "timestamp": [
                "2024-01-01 10:00:00",
                "2024-01-01 11:00:00",
                "2024-01-01 12:00:00"
            ]
        }).with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
        
        books = pl.DataFrame({
            "book_id": [101, 102, 103],
            "title": ["Book A", "Book B", "Book C"],
            "author": ["Author A", "Author B", "Author C"],
            "category": ["Fiction", "Non-Fiction", "Fiction"],
            "base_price": [10.0, 15.0, 12.0]
        })
        
        result = top_books(transactions, books)
        
        # Should be sorted by revenue descending
        revenues = result["revenue"].to_list()
        assert revenues == sorted(revenues, reverse=True)


class TestLoadTables:
    """Test load_tables function"""
    
    def test_load_tables_returns_paths(self):
        """Test that load_tables returns correct file paths"""
        result = load_tables()
        
        assert isinstance(result, dict)
        assert "users" in result
        assert "transactions" in result
        assert "books" in result
        assert result["users"].endswith("users.csv")
        assert result["transactions"].endswith("transactions.csv")
        assert result["books"].endswith("books.csv")


class TestTransformData:
    """Test transform_data function"""
    
    def test_transform_data_flow(self, mocker):
        """Test the complete transform_data flow"""
        # Setup mocks
        mock_load_data = mocker.patch('sue_books_org.dags.tables.load_data')
        mock_clean_users = mocker.patch('sue_books_org.dags.tables.clean_users_data')
        mock_clean_transactions = mocker.patch('sue_books_org.dags.tables.clean_transactions_data')
        mock_clean_books = mocker.patch('sue_books_org.dags.tables.clean_books_data')
        mock_daily_sales = mocker.patch('sue_books_org.dags.tables.daily_sales_table')
        mock_top_books = mocker.patch('sue_books_org.dags.tables.top_books')
        mocker.patch('sue_books_org.dags.tables.os.makedirs')
        mocker.patch.object(pl.DataFrame, "write_parquet", autospec=True)
        
        mock_users = pl.DataFrame({"id": [1], "name": ["Alice"], "email": ["alice@test.com"],
                                   "signup_date": ["2024-01-01"], "social_security_number": ["123456789"],
                                   "location": ["NYC"]})
        mock_transactions = pl.DataFrame({"transaction_id": [1], "user_id": [1], "book_id": [101],
                                          "amount": [10.0], "timestamp": ["2024-01-01 10:00:00"]})
        mock_books = pl.DataFrame({"book_id": [101], "title": ["Book A"], "author": ["Author A"],
                                   "category": ["Fiction"], "base_price": [10.0]})
        
        mock_load_data.return_value = mock_users
        mock_clean_users.return_value = mock_users
        mock_clean_transactions.return_value = mock_transactions
        mock_clean_books.return_value = mock_books
        mock_daily_sales.return_value = pl.DataFrame({"date_key": [20240101], "revenue": [10.0],
                                                       "num_transactions": [1], "active_users": [1],
                                                       "date": ["2024-01-01"]})
        mock_top_books.return_value = pl.DataFrame({"book_id": [101], "revenue": [10.0],
                                                     "num_sales": [1], "unique_buyers": [1]})
        
        result = transform_data("users.csv", "transactions.csv", "books.csv")
        
        assert isinstance(result, dict)
        assert "users" in result
        assert "transactions" in result
        assert "books" in result
        assert "daily_sales" in result
        assert "top_book" in result
        
        # Verify all cleaning functions were called
        mock_clean_users.assert_called_once()
        mock_clean_transactions.assert_called_once()
        mock_clean_books.assert_called_once()
        mock_daily_sales.assert_called_once()
        mock_top_books.assert_called_once()


class TestCreateDimTables:
    """Test create_dim_tables function"""
    
    def test_create_dim_tables_creates_dim_user(self):
        """Test creation of dim_user table"""
        users = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "email": ["alice@test.com", "bob@test.com", "charlie@test.com"],
            "signup_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "social_security_number": [123456789, 987654321, 456789123],
            "location": ["NYC", "LA", "Chicago"]
        }).with_columns(pl.col("signup_date").str.strptime(pl.Date, "%Y-%m-%d"))
        
        transactions = pl.DataFrame({
            "transaction_id": [1],
            "user_id": [1],
            "book_id": [101],
            "amount": [10.0],
            "timestamp": ["2024-01-01 10:00:00"]
        }).with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
        
        books = pl.DataFrame({
            "book_id": [101],
            "title": ["Book A"],
            "author": ["Author A"],
            "category": ["Fiction"],
            "base_price": [10.0]
        })
        
        result = create_dim_tables(users, books, transactions)
        
        assert "dim_user" in result
        assert "user_sk" in result["dim_user"].columns
        assert "user_id" in result["dim_user"].columns
        assert result["dim_user"].height == 3
    
    def test_create_dim_tables_creates_dim_book(self):
        """Test creation of dim_book table"""
        users = pl.DataFrame({
            "id": [1],
            "name": ["Alice"],
            "email": ["alice@test.com"],
            "signup_date": ["2024-01-01"],
            "social_security_number": [123456789],
            "location": ["NYC"]
        }).with_columns(pl.col("signup_date").str.strptime(pl.Date, "%Y-%m-%d"))
        
        transactions = pl.DataFrame({
            "transaction_id": [1],
            "user_id": [1],
            "book_id": [101],
            "amount": [10.0],
            "timestamp": ["2024-01-01 10:00:00"]
        }).with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
        
        books = pl.DataFrame({
            "book_id": [101, 102, 103],
            "title": ["Book A", "Book B", "Book C"],
            "author": ["Author A", "Author B", "Author C"],
            "category": ["Fiction", "Non-Fiction", "Fiction"],
            "base_price": [10.0, 15.0, 12.0]
        })
        
        result = create_dim_tables(users, books, transactions)
        
        assert "dim_books" in result
        assert "book_sk" in result["dim_books"].columns
        assert "book_id" in result["dim_books"].columns
        assert result["dim_books"].height == 3
    
    def test_create_dim_tables_creates_dim_date(self):
        """Test creation of dim_date table"""
        users = pl.DataFrame({
            "id": [1],
            "name": ["Alice"],
            "email": ["alice@test.com"],
            "signup_date": ["2024-01-01"],
            "social_security_number": [123456789],
            "location": ["NYC"]
        }).with_columns(pl.col("signup_date").str.strptime(pl.Date, "%Y-%m-%d"))
        
        transactions = pl.DataFrame({
            "transaction_id": [1, 2, 3],
            "user_id": [1, 1, 1],
            "book_id": [101, 102, 103],
            "amount": [10.0, 20.0, 30.0],
            "timestamp": [
                "2024-01-01 10:00:00",
                "2024-01-02 11:00:00",
                "2024-01-03 12:00:00"
            ]
        }).with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
        
        books = pl.DataFrame({
            "book_id": [101],
            "title": ["Book A"],
            "author": ["Author A"],
            "category": ["Fiction"],
            "base_price": [10.0]
        })
        
        result = create_dim_tables(users, books, transactions)
        
        assert "dim_date" in result
        assert "date_sk" in result["dim_date"].columns
        assert "date" in result["dim_date"].columns
        assert "year" in result["dim_date"].columns
        assert "quarter" in result["dim_date"].columns
        assert "month" in result["dim_date"].columns
        assert result["dim_date"].height == 3


class TestCreateFactTable:
    """Test create_fact_table function"""
    
    def test_create_fact_table_joins_with_dimensions(self):
        """Test that fact table joins with all dimension tables"""
        transactions = pl.DataFrame({
            "transaction_id": [1, 2, 3],
            "user_id": [1, 1, 2],
            "book_id": [101, 102, 101],
            "amount": [10.0, 20.0, 15.0],
            "timestamp": [
                "2024-01-01 10:00:00",
                "2024-01-01 11:00:00",
                "2024-01-02 12:00:00"
            ]
        }).with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
        
        dim_user = pl.DataFrame({
            "user_sk": [1, 2],
            "user_id": [1, 2],
            "name": ["Alice", "Bob"],
            "email": ["alice@test.com", "bob@test.com"],
            "location": ["NYC", "LA"],
            "signup_date": ["2024-01-01", "2024-01-01"]
        }).with_columns(pl.col("signup_date").str.strptime(pl.Date, "%Y-%m-%d"))
        
        dim_book = pl.DataFrame({
            "book_sk": [1, 2],
            "book_id": [101, 102],
            "title": ["Book A", "Book B"],
            "author": ["Author A", "Author B"],
            "category": ["Fiction", "Non-Fiction"],
            "base_price": [10.0, 15.0]
        })
        
        dim_date = pl.DataFrame({
            "date_sk": [20240101, 20240102],
            "date": ["2024-01-01", "2024-01-02"],
            "year": [2024, 2024],
            "quarter": [1, 1],
            "month": [1, 1],
            "week": [1, 1],
            "day_of_week": [1, 2]
        }).with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))
        
        result = create_fact_table(transactions, dim_user, dim_book, dim_date)
        
        assert "transaction_id" in result.columns
        assert "user_sk" in result.columns
        assert "book_sk" in result.columns
        assert "date_sk" in result.columns
        assert "amount" in result.columns
        assert result.height == 3
    
    def test_create_fact_table_inner_join_behavior(self):
        """Test that fact table only includes matching records"""
        transactions = pl.DataFrame({
            "transaction_id": [1, 2],
            "user_id": [1, 999],  # user 999 doesn't exist in dim_user
            "book_id": [101, 101],
            "amount": [10.0, 20.0],
            "timestamp": ["2024-01-01 10:00:00", "2024-01-01 11:00:00"]
        }).with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
        
        dim_user = pl.DataFrame({
            "user_sk": [1],
            "user_id": [1],
            "name": ["Alice"],
            "email": ["alice@test.com"],
            "location": ["NYC"],
            "signup_date": ["2024-01-01"]
        }).with_columns(pl.col("signup_date").str.strptime(pl.Date, "%Y-%m-%d"))
        
        dim_book = pl.DataFrame({
            "book_sk": [1],
            "book_id": [101],
            "title": ["Book A"],
            "author": ["Author A"],
            "category": ["Fiction"],
            "base_price": [10.0]
        })
        
        dim_date = pl.DataFrame({
            "date_sk": [20240101],
            "date": ["2024-01-01"],
            "year": [2024],
            "quarter": [1],
            "month": [1],
            "week": [1],
            "day_of_week": [1]
        }).with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))
        
        result = create_fact_table(transactions, dim_user, dim_book, dim_date)
        
        # Should only have 1 record (user 999 is filtered out)
        assert result.height == 1
        assert 1 in result["user_sk"].to_list()


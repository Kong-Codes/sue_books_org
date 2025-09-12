import os
import os.path
import polars as pl

from dotenv import load_dotenv
from polars import DataFrame

from utils import get_logger

load_dotenv()

log = get_logger(__name__, log_file="logs/table_ops.log")

DATASET_DIR = "/opt/airflow/datasets"
users_path = os.path.join(DATASET_DIR, "users.csv")
transactions_path = os.path.join(DATASET_DIR, "transactions.csv")
books_path = os.path.join(DATASET_DIR, "books.csv")


# Data ingestion
def load_data(data):
    df = pl.read_csv(data)
    return df


def clean_users_data(users) -> DataFrame:
    try:
        users = users.with_columns(
            pl.col("signup_date").str.replace("2024-13-45", "2024-12-30").cast(pl.Date)
        )
        users = users.with_columns(
            pl.col("email").str.contains("@").alias("is_email")
        )
        users = users.filter(pl.col("is_email"))
        users = users.with_columns(
            pl.col("social_security_number").str.replace("123-45-INVALID", 0)
        )
        users = users.with_columns(
            pl.col("social_security_number").str.replace_all("-", "").cast(pl.Int64)
        )
        users = users.drop("is_email")
        log.info(f"Users data cleaned successfully. Total records: {users.height}")
        return users
    except Exception as e:
        log.error(f"Error cleaning users data: {e}", exc_info=True)
        raise


def clean_transactions_data(transactions, users) -> DataFrame:
    try:
        transactions = transactions.with_columns(
            pl.col("book_id").replace("", None)
        )
        transactions = transactions.with_columns(
            pl.col("user_id").str.replace("USER_", "").cast(pl.Int64),
            pl.col("book_id").cast(pl.Float64).cast(pl.Int64),
            pl.col("timestamp").cast(pl.Utf8)
            .str.strip_chars()
            .replace("", None)
            .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
        )
        transactions = transactions.with_columns(
            pl.col("book_id").fill_null(0)
        )
        transactions = transactions.with_columns(
            pl.col("timestamp").fill_null(strategy="forward")
        )
        transactions = transactions.filter(
            (pl.col("amount").abs() > 1e-9)
        )
        transactions = transactions.filter(pl.col("amount") >= 0)
        transactions = transactions.filter(pl.col("book_id") != 0)
        transactions_clean = transactions.join(
            users.select("id").unique(),
            left_on="user_id",
            right_on="id",
            how="semi"
        )
        log.info(f"Transactions data cleaned successfully. Total records: {transactions_clean.height}")
        return transactions_clean
    except Exception as e:
        log.error(f"Error cleaning transactions data: {e}", exc_info=True)
        raise


def clean_books_data(books) -> DataFrame:
    return books


def daily_sales_table(transactions: pl.DataFrame) -> DataFrame:
    """
    Build daily revenue / tx count / active users, plus a YYYYMMDD date_key.
    """
    try:
        daily_sales = (
            transactions
            .with_columns(pl.col("timestamp").dt.date().alias("date"))
            .group_by("date")
            .agg([
                pl.col("amount").sum().alias("revenue"),
                pl.len().alias("num_transactions"),
                pl.col("user_id").n_unique().alias("active_users"),
            ])
            .with_columns(
                (
                    pl.col("date").dt.year() * 10000
                    + pl.col("date").dt.month() * 100
                    + pl.col("date").dt.day()
                ).cast(pl.Int32).alias("date_key")
            )
            .select(["date_key", "revenue", "num_transactions", "active_users", "date"])
            .unique(subset=["date_key"], keep="last")
            .sort("date")
        )
        log.info(f"Daily sales table created successfully. Total records: {daily_sales.height}")
        return daily_sales
    except Exception as e:
        log.error(f"Error creating daily sales table: {e}", exc_info=True)
        raise


def top_books(transactions, books) -> DataFrame:
    try:
        top_book = (
            transactions
            .group_by("book_id")
            .agg([
                pl.col("amount").sum().alias("revenue"),
                pl.len().alias("num_sales"),
                pl.col("user_id").n_unique().alias("unique_buyers"),
            ])
            .join(
                books.select("book_id"),
                on="book_id",
                how="left",
            )
            .sort(by=[pl.col("revenue"), pl.col("num_sales")], descending=[True, True])

        )
        return top_book
    except Exception as e:
        log.error(f"Error creating top books table: {e}", exc_info=True)
        raise


def load_tables():
    return {"users": users_path, "transactions": transactions_path, "books": books_path}


def transform_data(users_data_path, transactions_data_path, books_data_path):
    try:
        users = load_data(users_data_path)
        transactions = load_data(transactions_data_path)
        books = load_data(books_data_path)
        log.info("Data loaded successfully.")

        users_clean = clean_users_data(users)
        transactions_clean = clean_transactions_data(transactions, users_clean)
        books_clean = clean_books_data(books)

        daily_sales = daily_sales_table(transactions_clean)
        top_book = top_books(transactions_clean, books_clean)

        log.info("Data cleaning and transformation complete.")

        # Save outputs to files
        base_dir = "/opt/airflow/datasets/processed"
        os.makedirs(base_dir, exist_ok=True)

        users_new_path = os.path.join(base_dir, "users_clean.parquet")
        transactions_new_path = os.path.join(base_dir, "transactions_clean.parquet")
        books_new_path = os.path.join(base_dir, "books_clean.parquet")
        daily_sales_path = os.path.join(base_dir, "daily_sales.parquet")
        top_book_path = os.path.join(base_dir, "top_book.parquet")

        users_clean.write_parquet(users_new_path, compression="snappy")
        transactions_clean.write_parquet(transactions_new_path, compression="snappy")
        books_clean.write_parquet(books_new_path, compression="snappy")
        daily_sales.write_parquet(daily_sales_path, compression="snappy")
        top_book.write_parquet(top_book_path, compression="snappy")

        log.info("Transformed data saved to files successfully.")
        return {
            'users': users_new_path,
            'transactions': transactions_new_path,
            'books': books_new_path,
            'daily_sales': daily_sales_path,
            'top_book': top_book_path
        }

    except Exception as e:
        log.error(f"Error in transform_data: {e}", exc_info=True)
        raise


def create_dim_tables(users, books, transactions):
    try:
        dim_user = (
            users
            .select(pl.col("id").alias("user_id"), "name", "email", "location", "signup_date")
            .unique(subset=["user_id"])
            .with_row_index("user_sk", offset=1)
        )

        dim_book = (
            books
            .select(pl.col("book_id"), "title", "author", "category", "base_price")
            .unique(subset=["book_id"])
            .with_row_index("book_sk", offset=1)
        )

        dim_date = (
            transactions
            .with_columns(pl.col("timestamp").dt.date().alias("date"))
            .select("date")
            .unique()
            .with_columns((pl.col("date").dt.year() * 10000
                           + pl.col("date").dt.month() * 100
                           + pl.col("date").dt.day()).alias("date_sk"))
            .with_columns([
                pl.col("date").dt.year().alias("year"),
                pl.col("date").dt.quarter().alias("quarter"),
                pl.col("date").dt.month().alias("month"),
                pl.col("date").dt.week().alias("week"),
                pl.col("date").dt.weekday().alias("day_of_week")
            ])
            .select(["date_sk", "date", "year", "quarter", "month", "week", "day_of_week"])
        )
        log.info("Dimension tables created successfully.")
        return {'dim_user': dim_user,
                'dim_books': dim_book,
                'dim_date': dim_date
                }
    except Exception as e:
        log.error(f"Error creating dimension tables: {e}", exc_info=True)
        raise


def create_fact_table(transactions, dim_user, dim_book, dim_date):
    try:
        fact_transactions = (
            transactions
            .with_columns(pl.col("timestamp").dt.date().alias("date"))
            .join(dim_user.select("user_sk", "user_id"), left_on="user_id", right_on="user_id", how="inner")
            .join(dim_book.select("book_sk", "book_id"), left_on="book_id", right_on="book_id", how="inner")
            .join(dim_date.select("date_sk", "date"), on="date", how="inner")
            .select([
                "transaction_id",
                "user_sk",
                "book_sk",
                "date_sk",
                pl.col("amount").alias("amount")
            ])
        )
        log.info(f"Fact table created successfully. Total records: {fact_transactions.height}")
        return fact_transactions
    except Exception as e:
        log.error(f"Error creating fact table: {e}", exc_info=True)
        raise



import os
import polars as pl
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

from tables import load_tables, transform_data, create_dim_tables, create_fact_table
from utils import upsert_from_df, create_tables, get_db



DB_URL = os.getenv("DATABASE_URL")
DB_NAME = os.getenv("DATABASE_NAME")

default_args = {
    'owner': 'sadique',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 10),
    'email': ['sadiquetimileyin@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    'Sue_books_data_pipeline',
    default_args=default_args,
    description='A simple DAG to upload data to the db',
    schedule_interval=timedelta(days=1),
    catchup=False,
)
def etl_pipeline():

    @task(task_id= 'extract_data')
    def extract_data():
        data = load_tables()
        return data

    @task(task_id= 'transform_data')
    def transform(**kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='extract_data')
        transformed_data = transform_data(df['users'], df['transactions'], df['books'])
        return transformed_data

    @task(task_id= 'load_data')
    def load_data(**kwargs):
        ti = kwargs['ti']
        transformed_data = ti.xcom_pull(task_ids='transform_data')
        users = pl.read_parquet(transformed_data['users'])
        books = pl.read_parquet(transformed_data['books'])
        transactions = pl.read_parquet(transformed_data['transactions'])
        daily_summary = pl.read_parquet(transformed_data['daily_sales'])
        top_books = pl.read_parquet(transformed_data['top_book'])
        dim_tables = create_dim_tables(users, books, transactions)
        fact_table = create_fact_table(transactions, dim_tables['dim_user'],
                                       dim_tables['dim_books'],
                                       dim_tables['dim_date'])

        create_tables(db_name=DB_NAME, db_url=DB_URL)
        conn = get_db(DB_URL)
        #oltp schema
        upsert_from_df(conn, users, 'users', ['id'], schema='oltp')
        upsert_from_df(conn, books, 'books', ['book_id'], schema='oltp')
        upsert_from_df(conn, transactions, 'transactions', ['transaction_id'], schema='oltp')

        #olap schema
        upsert_from_df(conn, dim_tables['dim_user'], 'dim_user', ['user_sk'], schema='olap')
        upsert_from_df(conn, dim_tables['dim_books'], 'dim_book', ['book_sk'], schema='olap')
        upsert_from_df(conn, dim_tables['dim_date'], 'dim_date', ['date_sk'], schema='olap')
        upsert_from_df(conn, fact_table, 'fact_transactions', ['transaction_id'], schema='olap')
        upsert_from_df(conn, daily_summary, 'fact_daily_sales', ['date_key'], schema='olap')
        upsert_from_df(conn, top_books, 'fact_book_sales', ['book_id'], schema='olap')


    extract_job = extract_data()
    transform_job = transform()
    load_job = load_data()

    extract_job >> transform_job >> load_job

etl_dag = etl_pipeline()



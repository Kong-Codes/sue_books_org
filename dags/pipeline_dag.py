import os
import polars as pl
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

from tables import load_tables, transform_data, create_dim_tables, create_fact_table
from utils import upsert_from_df, create_tables, get_db, get_logger, send_alert



DB_URL = os.getenv("DATABASE_URL")
DB_NAME = os.getenv("DATABASE_NAME")

# Setup logging for pipeline
pipeline_log = get_logger(__name__, log_file="logs/pipeline.log")


def on_task_success(context):
    """Callback for successful task completion."""
    ti = context["task_instance"]
    pipeline_log.info(f"Task {ti.task_id} in DAG {ti.dag_id} completed successfully")


def on_task_failure(context):
    """Callback for task failure with alerting."""
    ti = context["task_instance"]
    pipeline_log.error(f"Task {ti.task_id} in DAG {ti.dag_id} failed")
    send_alert(
        "Airflow Task Failure",
        {
            "task_id": ti.task_id,
            "dag_id": ti.dag_id,
            "execution_date": str(ti.execution_date),
            "log_url": ti.log_url if hasattr(ti, 'log_url') else None
        }
    )

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

    @task(task_id= 'extract_data', on_success_callback=on_task_success, on_failure_callback=on_task_failure)
    def extract_data():
        data = load_tables()
        pipeline_log.info(f"Extracted data: {len(data)} tables loaded")
        return data

    @task(task_id= 'transform_data', on_success_callback=on_task_success, on_failure_callback=on_task_failure)
    def transform(**kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='extract_data')
        transformed_data = transform_data(df['users'], df['transactions'], df['books'])
        pipeline_log.info(f"Transformed data: {len(transformed_data)} outputs created")
        return transformed_data

    @task(task_id= 'load_data', on_success_callback=on_task_success, on_failure_callback=on_task_failure)
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
        
        # Log metrics for data loaded
        total_rows = users.height + books.height + transactions.height + daily_summary.height + top_books.height
        pipeline_log.info(f"Data load completed: {total_rows} total rows loaded across all tables")
        
        # Alert if no data was loaded
        if total_rows == 0:
            pipeline_log.warning("No data loaded - this may indicate a pipeline issue")
            send_alert("Zero Data Loaded", {"stage": "load_data", "total_rows": 0})
        
        conn.close()


    extract_job = extract_data()
    transform_job = transform()
    load_job = load_data()

    extract_job >> transform_job >> load_job

etl_dag = etl_pipeline()



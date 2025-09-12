from tables import load_tables, transform_data, create_dim_tables, create_fact_table
from utils import upsert_from_df, get_db


def extract_data():
    data = load_tables()
    return data


def transform(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='extract_data')
    transformed_data = transform_data(df['users'], df['transactions'], df['books'])
    return transformed_data


def load_data(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data')
    dim_tables = create_dim_tables(transformed_data['users'], transformed_data['books'],
                                   transformed_data['transactions'])
    fact_table = create_fact_table(transformed_data['transactions'], dim_tables['dim_user'], dim_tables['dim_books'],
                                   dim_tables['dim_date'])
    conn = get_db()
    upsert_from_df(conn, dim_tables['dim_user'], 'dim_user', ['user_sk'])
    upsert_from_df(conn, dim_tables['dim_books'], 'dim_book', ['book_sk'])
    upsert_from_df(conn, dim_tables['dim_date'], 'dim_date', ['date_sk'])
    upsert_from_df(conn, fact_table, 'fact_sales', ['transaction_id'])
    conn.close()


"""
docker compose exec postgres psql -U sue_books -d postgres -c \
  "CREATE DATABASE sue_books OWNER sue_books;"
docker compose exec postgres \
  psql -U sue_books -d postgres -c "CREATE DATABASE metabase OWNER sue_books;"

"""


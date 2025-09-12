import os
from typing import List, Optional

import polars as pl
import psycopg2
from fastapi import FastAPI, HTTPException, Query
from datetime import date as Date
from pydantic import BaseModel


# ---------- Config ----------
SALES_DB_URL = os.environ.get("DATABASE_URL")
PROCESSED_DIR = os.environ.get("PROCESSED_DIR", "/opt/airflow/datasets/processed")


# ---------- FastAPI app ----------
app = FastAPI(title="Sales Analytics API", version="1.0.0")


# ---------- Pydantic models ----------
class DailySalesItem(BaseModel):
    date_key: int
    date: str
    revenue: float
    num_transactions: int
    active_users: int


class TopBookItem(BaseModel):
    book_id: int
    revenue: float
    num_sales: int
    unique_buyers: int


class PurchaseItem(BaseModel):
    transaction_id: int
    book_id: int
    amount: float
    timestamp: str


# ---------- Helpers ----------
def _db_conn() -> Optional[psycopg2.extensions.connection]:
    if not SALES_DB_URL:
        return None
    try:
        return psycopg2.connect(SALES_DB_URL)
    except Exception:
        return None


def _read_parquet(filename: str) -> pl.DataFrame:
    path = os.path.join(PROCESSED_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    return pl.read_parquet(path)


# ---------- Endpoints ----------
@app.get("/sales/daily", response_model=DailySalesItem)
def get_daily_sales(
    date: str = Query(..., description="Exact date in YYYY-MM-DD"),
):
    # validate date
    try:
        anchor_date = Date.fromisoformat(date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    conn = _db_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT d.date_sk AS date_key,
                           d."date"::text AS date,
                           ds.revenue::float,
                           ds.num_transactions::int,
                           ds.active_users::int
                    FROM olap.fact_daily_sales ds
                    JOIN olap.dim_date d ON d.date_sk = ds.date_key
                    WHERE d."date" = %s::date
                    LIMIT 1
                    """,
                    [anchor_date.isoformat()],
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="No sales found for the provided date")
                return DailySalesItem(
                    date_key=row[0], date=row[1], revenue=row[2], num_transactions=row[3], active_users=row[4]
                )
        finally:
            conn.close()

    # Fallback to parquet
    try:
        df = _read_parquet("daily_sales.parquet")
        df = df.filter(pl.col("date") == anchor_date)
        if df.height == 0:
            raise HTTPException(status_code=404, detail="No sales found for the provided date")
        row = df.to_dicts()[0]
        return DailySalesItem(
            date_key=int(row["date_key"]),
            date=str(row["date"]),
            revenue=float(row["revenue"]),
            num_transactions=int(row["num_transactions"]),
            active_users=int(row["active_users"]),
        )
    except FileNotFoundError:
        raise HTTPException(status_code=503, detail="No data available (DB and parquet missing)")


@app.get("/books/top", response_model=List[TopBookItem])
def get_top_books(limit: int = Query(5, ge=1, le=100)):
    conn = _db_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT b.book_id::int, f.revenue::float, f.num_sales::int, f.unique_buyers::int
                    FROM olap.fact_book_sales f
                    JOIN olap.dim_book b ON b.book_sk = f.book_id
                    ORDER BY f.revenue DESC, f.num_sales DESC
                    LIMIT %s
                    """,
                    [limit],
                )
                rows = cur.fetchall()
                return [
                    TopBookItem(book_id=r[0], revenue=r[1], num_sales=r[2], unique_buyers=r[3])
                    for r in rows
                ]
        finally:
            conn.close()

    # Fallback to parquet
    try:
        df = _read_parquet("top_book.parquet")
        df = df.sort(["revenue", "num_sales"], descending=[True, True]).head(limit)
        df = df.select(["book_id", "revenue", "num_sales", "unique_buyers"])  # ensure columns
        return [
            TopBookItem(
                book_id=int(row["book_id"]),
                revenue=float(row["revenue"]),
                num_sales=int(row["num_sales"]),
                unique_buyers=int(row["unique_buyers"]),
            )
            for row in df.to_dicts()
        ]
    except FileNotFoundError:
        raise HTTPException(status_code=503, detail="No data available (DB and parquet missing)")


@app.get("/users/{user_id}/purchases", response_model=List[PurchaseItem])
def get_user_purchases(user_id: int, limit: int = Query(100, ge=1, le=5000)):
    conn = _db_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ft.transaction_id::bigint,
                           db.book_id::int,
                           ft.amount::float,
                           dd."date"::text || 'T00:00:00Z' as timestamp
                    FROM olap.fact_transactions ft
                    JOIN olap.dim_user du ON du.user_sk = ft.user_sk
                    JOIN olap.dim_book db ON db.book_sk = ft.book_sk
                    JOIN olap.dim_date dd ON dd.date_sk = ft.date_sk
                    WHERE du.user_id = %s
                    ORDER BY ft.transaction_id DESC
                    LIMIT %s
                    """,
                    [user_id, limit],
                )
                rows = cur.fetchall()
                return [
                    PurchaseItem(
                        transaction_id=int(r[0]), book_id=int(r[1]), amount=float(r[2]), timestamp=str(r[3])
                    )
                    for r in rows
                ]
        finally:
            conn.close()

    # Fallback to parquet
    try:
        tx = _read_parquet("transactions_clean.parquet")
        tx = (
            tx.filter(pl.col("user_id") == user_id)
              .sort("timestamp", descending=True)
              .head(limit)
              .select(["transaction_id", "book_id", "amount", "timestamp"])  # ensure columns
        )
        # Ensure timestamp becomes string
        out = []
        for row in tx.to_dicts():
            ts_val = row["timestamp"]
            if isinstance(ts_val, (str,)):
                ts_str = ts_val
            else:
                ts_str = str(ts_val)
            out.append(
                PurchaseItem(
                    transaction_id=int(row["transaction_id"]),
                    book_id=int(row["book_id"]),
                    amount=float(row["amount"]),
                    timestamp=ts_str,
                )
            )
        return out
    except FileNotFoundError:
        raise HTTPException(status_code=503, detail="No data available (DB and parquet missing)")


# Uvicorn entrypoint
def get_app():
    return app



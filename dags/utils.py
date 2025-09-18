import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Sequence, Optional
import polars as pl

import numpy as np
import psycopg2
import psycopg2.extensions
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extras import execute_values
import requests
import time
from datetime import datetime, timezone

load_dotenv()
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
psycopg2.extensions.register_adapter(np.float64, psycopg2._psycopg.AsIs)

SQL_DIR = Path(os.environ.get("SQL_DIR", "/opt/airflow/sql"))
SQL_OLAP_FILE = SQL_DIR / "olap_schema.sql"
SQL_OLTP_FILE = SQL_DIR / "oltp_schema.sql"


def get_logger(name: str, log_file: str = "app.log"):
    """
    Creates a logger that writes to console and rotating file.

    Args:
        name (str): Logger name (usually __name__).
        log_file (str): Path to the log file.
    """
    # Ensure log folder exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True) if "/" in log_file else None

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Capture all levels

    # Prevent duplicate handlers if logger is called multiple times
    if logger.hasHandlers():
        return logger

    # --- Console handler ---
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # --- File handler (rotates when file > 5MB, keeps 5 backups) ---
    file_handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=5)
    file_handler.setLevel(logging.DEBUG)

    # --- Formatter ---
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # --- Add handlers ---
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


log = get_logger(__name__, log_file="logs/db_ops.log")

logg = get_logger(__name__, log_file="logs/alerts.log")


def _truncate(s: str, limit: int) -> str:
    return s if len(s) <= limit else s[: limit - 1] + "…"


def _to_iso8601(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


def send_alert(title: str, details: dict, level: str = "info") -> bool:
    """Send an alert to Discord via webhook URL in ALERT_WEBHOOK_URL."""
    webhook_url = os.environ.get("ALERT_WEBHOOK_URL")
    if not webhook_url:
        logg.warning("ALERT_WEBHOOK_URL not set; skipping alert.")
        return False

    # Build fields from details (Discord: 25 fields max; name<=256, value<=1024)
    fields = []
    for k, v in (details or {}).items():
        if len(fields) >= 25:
            break
        name = _truncate(str(k), 256)
        value = _truncate(str(v), 1024)
        fields.append({"name": name, "value": value, "inline": True})

    color_map = {"info": 0x2563EB, "warn": 0xF59E0B, "error": 0xDC2626}
    ts_ms = int(time.time() * 1000)

    payload = {
        "username": "Alert Bot",
        "embeds": [{
            "title": _truncate(title, 256),
            "timestamp": _to_iso8601(ts_ms),  # ISO8601 required by Discord
            "color": color_map.get(level, color_map["info"]),
            "fields": fields,
        }],
        # avoid accidental @everyone/@here pings
        "allowed_mentions": {"parse": []},
    }

    try:
        resp = requests.post(webhook_url, json=payload, timeout=5)
        if resp.status_code == 429:
            # Respect Discord rate limits
            retry_after = resp.json().get("retry_after", 1)
            logg.warning(f"Rate limited by Discord. Retry after {retry_after}s.")
            return False
        resp.raise_for_status()
        logg.info(f"Discord alert sent: {title}")
        return True
    except requests.RequestException as exc:
        logg.error(f"Failed to send Discord alert '{title}': {exc}", exc_info=True)
        return False


def log_call(logger, op: str):
    """Decorator to log function execution time and success/failure."""

    def decorator(fn):
        def wrapper(*args, **kwargs):
            start = time.time()
            try:
                result = fn(*args, **kwargs)
                duration_ms = int((time.time() - start) * 1000)
                logger.info(f"{op} completed successfully in {duration_ms}ms")
                return result
            except Exception as exc:
                duration_ms = int((time.time() - start) * 1000)
                logger.error(f"{op} failed in {duration_ms}ms: {exc}", exc_info=True)
                raise

        return wrapper

    return decorator


def check_and_create_db(target_dbname: str, url_env_var: str):
    """
    Connect to a PostgreSQL admin DB using a URL from an env var,
    and create `target_dbname` if it doesn't exist.

    Parameters:
    - target_dbname (str): Name of the DB to check/create
    - url_env_var (str): Name of the env var that contains the full DB URL (e.g., 'SALES_DB_ADMIN_URL')

    Required:
    - An environment variable named `url_env_var` must exist and hold a valid PostgreSQL URL to an admin DB (e.g., .../postgres)
    """
    load_dotenv()

    admin_db_url = url_env_var
    if not admin_db_url:
        raise ValueError(f"Environment variable '{url_env_var}' is not set.")

    if not admin_db_url.lower().endswith("/sales_db"):
        log.warning("Note: Admin DB URL usually ends with '/postgres' for DB management.")

    try:
        conn = psycopg2.connect(admin_db_url)
        conn.autocommit = True
        cur = conn.cursor()

        # Check if a database already exists
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", [target_dbname])
        exists = cur.fetchone()

        if not exists:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target_dbname)))
            log.info(f"Database '{target_dbname}' created.")
        else:
            log.warning(f"Database '{target_dbname}' already exists.")

        cur.close()
        conn.close()

    except Exception as e:
        log.error(f"Error while checking/creating DB '{target_dbname}': {e}", exc_info=True)


def get_db(db_url):
    """Get a connection to the target DB specified by the SALES_DB_URL env var."""
    if not db_url:
        raise ValueError("Environment variable 'SALES_DB_URL' is not set.")
    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        log.error(f"Error connecting to DB: {e}", exc_info=True)
        raise


def upsert_from_df(
        conn,
        df: pl.DataFrame,
        table_name: str,
        conflict_columns: Sequence[str],
        update_columns: Optional[Sequence[str]] = None,
        schema: str = "public",
        chunk_size: int = 10_000,
):
    """
    Upserts a Polars DataFrame into a PostgreSQL table.

    Parameters:
    - conn: psycopg2 connection object
    - df: polars.DataFrame
    - table_name: target table name (without schema)
    - conflict_columns: columns used in ON CONFLICT (...)
    - update_columns: columns to update on conflict; if None, all except conflict cols
    - schema: schema name (default: public)
    - chunk_size: number of rows per batch for execute_values
    """

    # ---- helpers ----
    def q(identifier: str) -> str:
        """Quote an identifier safely for Postgres."""
        return '"' + identifier.replace('"', '""') + '"'

    df = df.unique(subset=conflict_columns, keep="last")
    if df is None or df.height == 0:
        log.warning(f"Skipping {schema}.{table_name}: DataFrame is empty or None.")
        return

    # validate columns
    cols = list(df.columns)
    missing_conflict = [c for c in conflict_columns if c not in cols]
    if missing_conflict:
        raise ValueError(f"conflict_columns not in DataFrame: {missing_conflict}")

    # default update set = all non-conflict columns
    if update_columns is None:
        update_columns = [c for c in cols if c not in conflict_columns]

    # Build SQL
    qualified_table = f"{q(schema)}.{q(table_name)}"
    col_list_sql = ", ".join(q(c) for c in cols)
    conflict_sql = ", ".join(q(c) for c in conflict_columns)

    if update_columns:
        update_sql = ", ".join(f"{q(c)} = EXCLUDED.{q(c)}" for c in update_columns)
        on_conflict_sql = f"ON CONFLICT ({conflict_sql}) DO UPDATE SET {update_sql}"
    else:
        # no columns to update -> DO NOTHING
        on_conflict_sql = f"ON CONFLICT ({conflict_sql}) DO NOTHING"

    insert_sql = (
        f"INSERT INTO {qualified_table} ({col_list_sql}) VALUES %s {on_conflict_sql};"
    )

    total = df.height
    log.info(f"Preparing to upsert {total} rows into {schema}.{table_name}...")

    try:
        with conn.cursor() as cur:
            # chunk to avoid huge in-memory lists
            for start in range(0, total, chunk_size):
                end = min(start + chunk_size, total)
                chunk = df.slice(start, end - start)

                # rows() -> list[tuple], with Python-native types (None for nulls)
                values = chunk.rows()
                if not values:
                    continue

                execute_values(cur, insert_sql, values, page_size=chunk_size)

        conn.commit()
        log.info(f"{total} records upserted into {schema}.{table_name}")
    except Exception as e:
        log.error(f"Failed to upsert into {schema}.{table_name}: {e}", exc_info=True)
        conn.rollback()
        raise


def create_tables(db_name, db_url):
    check_and_create_db(db_name, db_url)
    conn = get_db(db_url)
    cur = conn.cursor()
    try:
        with open(SQL_OLAP_FILE, "r") as f:
            cur.execute(f.read())
        with open(SQL_OLTP_FILE, "r") as f:
            cur.execute(f.read())
        conn.commit()
        log.info("Tables created successfully.")
        cur.execute("SELECT * FROM information_schema.tables WHERE table_schema = 'olap'")
        olap_tables = cur.fetchall()

        log.info(f"OLAP Tables: {[t[2] for t in olap_tables]}")
        conn.close()
    except Exception as e:
        log.error(f"Error creating tables: {e}", exc_info=True)
        conn.rollback()

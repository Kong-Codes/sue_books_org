-- OLAP (analytics-ready)
CREATE SCHEMA IF NOT EXISTS olap;

DROP TABLE IF EXISTS olap.dim_user CASCADE;
DROP TABLE IF EXISTS olap.dim_book CASCADE;
DROP TABLE IF EXISTS olap.dim_date CASCADE;
DROP TABLE IF EXISTS olap.fact_transactions CASCADE;
DROP TABLE IF EXISTS olap.fact_daily_sales CASCADE;
DROP TABLE IF EXISTS olap.fact_book_sales CASCADE;

-- Dimensions
CREATE TABLE IF NOT EXISTS olap.dim_user (
  user_sk   BIGSERIAL PRIMARY KEY,
  user_id    BIGINT UNIQUE NOT NULL,
  name       TEXT,
  email      TEXT,
  location   TEXT,
  signup_date DATE
);

CREATE TABLE IF NOT EXISTS olap.dim_book (
  book_sk   BIGSERIAL PRIMARY KEY,
  book_id    BIGINT UNIQUE NOT NULL,
  title      TEXT,
  author     TEXT,
  category   TEXT,
  base_price NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS olap.dim_date (
  date_sk     INT PRIMARY KEY,
  "date"       DATE NOT NULL,
  "year"       INT  NOT NULL,
  quarter      INT  NOT NULL CHECK (quarter BETWEEN 1 AND 4),
  "month"    INT  NOT NULL CHECK ("month" BETWEEN 1 AND 12),
  week     INT  CHECK (week BETWEEN 1 AND 53),
  day_of_week  INT  CHECK (day_of_week BETWEEN 1 AND 7)
);

-- Transaction fact 
CREATE TABLE IF NOT EXISTS olap.fact_transactions (
  transaction_id BIGINT PRIMARY KEY,
  user_sk  BIGINT NOT NULL REFERENCES olap.dim_user(user_sk),
  book_sk  BIGINT NOT NULL REFERENCES olap.dim_book(book_sk),
  date_sk  INT    NOT NULL REFERENCES olap.dim_date(date_sk),
  amount    NUMERIC(12,2) NOT NULL
);

CREATE INDEX IF NOT EXISTS ftx_user_key ON olap.fact_transactions(user_sk);
CREATE INDEX IF NOT EXISTS ftx_book_key ON olap.fact_transactions(book_sk);
CREATE INDEX IF NOT EXISTS ftx_date_key ON olap.fact_transactions(date_sk);


-- Daily Sales Summary
CREATE TABLE IF NOT EXISTS olap.fact_daily_sales (
  date_key         INT PRIMARY KEY REFERENCES olap.dim_date(date_sk),
  revenue          NUMERIC(14,2) NOT NULL,
  num_transactions BIGINT        NOT NULL,
  active_users     BIGINT        NOT NULL,
  "date"            DATE         NOT NULL
);

-- Top Books Summary
CREATE TABLE IF NOT EXISTS olap.fact_book_sales (
  book_id      BIGINT PRIMARY KEY REFERENCES olap.dim_book(book_sk),
  revenue       NUMERIC(14,2) NOT NULL,
  num_sales     BIGINT        NOT NULL,
  unique_buyers BIGINT        NOT NULL
);

-- OLTP (source-of-truth for the app)
CREATE SCHEMA IF NOT EXISTS oltp;

--DROP TABLE IF EXISTS oltp.users CASCADE;
--DROP TABLE IF EXISTS oltp.books CASCADE;
--DROP TABLE IF EXISTS oltp.transactions CASCADE;

-- Reference tables
CREATE TABLE IF NOT EXISTS oltp.users (
  id            BIGSERIAL PRIMARY KEY,
  name          TEXT NOT NULL,
  email         TEXT,
  location      TEXT,
  signup_date   DATE,
  social_security_number      BIGINT
);

CREATE TABLE IF NOT EXISTS oltp.books (
  book_id           BIGSERIAL PRIMARY KEY,
  title             TEXT NOT NULL,
  category          TEXT,
  base_price        NUMERIC(12,2) CHECK (base_price >= 0),
  author            TEXT,
  isbn              TEXT UNIQUE,
  publication_year  INT,
  pages             INT CHECK (pages >= 0),
  publisher         TEXT
);

-- Transactions (facts at the operational level)
CREATE TABLE IF NOT EXISTS oltp.transactions (
  transaction_id  BIGSERIAL PRIMARY KEY,
  user_id         BIGINT  NOT NULL REFERENCES oltp.users(id)
                   ON UPDATE CASCADE ON DELETE RESTRICT,
  book_id         BIGINT  NOT NULL REFERENCES oltp.books(book_id)
                   ON UPDATE CASCADE ON DELETE RESTRICT,
  amount          NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
  "timestamp"     TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_tx_user    ON oltp.transactions (user_id);
CREATE INDEX IF NOT EXISTS idx_tx_book    ON oltp.transactions (book_id);
CREATE INDEX IF NOT EXISTS idx_tx_time    ON oltp.transactions ("timestamp");

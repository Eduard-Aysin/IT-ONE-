CREATE TABLE IF NOT EXISTS target_table (
    id           SERIAL PRIMARY KEY,
    file_name    TEXT,
    order_id     INTEGER,
    product_name TEXT,
    quantity     INTEGER,
    price        NUMERIC(10,2),
    customer_id  INTEGER,
    order_date   DATE,
    loaded_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS processed_files (
    id           SERIAL PRIMARY KEY,
    file_name    TEXT UNIQUE,
    processed_at TIMESTAMP DEFAULT NOW(),
    status       TEXT DEFAULT 'processed';
);

CREATE TABLE IF NOT EXISTS dq_checks (
    id           SERIAL PRIMARY KEY,
    file_name    TEXT,
    rows_in_file INTEGER,
    rows_in_db   INTEGER,
    is_valid     BOOLEAN,
    checked_at   TIMESTAMP DEFAULT NOW()
);

ALTER TABLE processed_files ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'processed';
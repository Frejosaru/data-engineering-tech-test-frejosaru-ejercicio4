-- DDL para modelo de datos (Star Schema + SCD2 + partición)

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS archive;

-- Dimensiones con SCD2
CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_sk        BIGSERIAL PRIMARY KEY,
    customer_bk        TEXT NOT NULL,
    country            TEXT,
    city               TEXT,
    valid_from         TIMESTAMP NOT NULL,
    valid_to           TIMESTAMP,
    is_current         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMP DEFAULT NOW(),
    updated_at         TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_customer_bk_current
    ON dwh.dim_customer (customer_bk)
    WHERE is_current;

CREATE TABLE IF NOT EXISTS dwh.dim_merchant (
    merchant_sk        BIGSERIAL PRIMARY KEY,
    merchant_bk        TEXT NOT NULL,
    category           TEXT,
    country            TEXT,
    city               TEXT,
    valid_from         TIMESTAMP NOT NULL,
    valid_to           TIMESTAMP,
    is_current         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMP DEFAULT NOW(),
    updated_at         TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_merchant_bk_current
    ON dwh.dim_merchant (merchant_bk)
    WHERE is_current;

-- Dimensiones simples
CREATE TABLE IF NOT EXISTS dwh.dim_payment_method (
    payment_method_sk  BIGSERIAL PRIMARY KEY,
    method_code        TEXT UNIQUE NOT NULL,
    card_type          TEXT
);

CREATE TABLE IF NOT EXISTS dwh.dim_currency (
    currency_sk        BIGSERIAL PRIMARY KEY,
    currency_code      TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key           INTEGER PRIMARY KEY,
    date               DATE NOT NULL,
    year               SMALLINT,
    quarter            SMALLINT,
    month              SMALLINT,
    day                SMALLINT,
    weekday            SMALLINT
);

-- Tabla de hechos particionada
CREATE TABLE IF NOT EXISTS dwh.fact_transactions (
    transaction_sk     BIGSERIAL PRIMARY KEY,
    transaction_id     TEXT NOT NULL,
    customer_sk        BIGINT NOT NULL REFERENCES dwh.dim_customer(customer_sk),
    merchant_sk        BIGINT NOT NULL REFERENCES dwh.dim_merchant(merchant_sk),
    payment_method_sk  BIGINT NOT NULL REFERENCES dwh.dim_payment_method(payment_method_sk),
    currency_sk        BIGINT NOT NULL REFERENCES dwh.dim_currency(currency_sk),
    date_key           INTEGER NOT NULL REFERENCES dwh.dim_date(date_key),
    amount             NUMERIC(18, 4) NOT NULL,
    status             TEXT,
    transaction_ts     TIMESTAMP NOT NULL,
    created_at         TIMESTAMP DEFAULT NOW(),
    UNIQUE (transaction_id)
) PARTITION BY RANGE (transaction_ts);

CREATE TABLE IF NOT EXISTS staging.transactions_raw (
    transaction_id     TEXT,
    customer_id        TEXT,
    merchant_id        TEXT,
    transaction_ts     TIMESTAMP,
    amount             NUMERIC(18,4),
    currency           TEXT,
    status             TEXT,
    country            TEXT,
    city               TEXT,
    payment_method     TEXT,
    card_type          TEXT,
    category           TEXT
);

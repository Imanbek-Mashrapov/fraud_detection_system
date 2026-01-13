CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS features;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS meta;

-- raw schema
CREATE TABLE IF NOT EXISTS raw.users (
    user_id           TEXT,
    registration_date TIMESTAMP,
    home_country      TEXT,
    risk_segment      TEXT,
    ingestion_ts      TIMESTAMP DEFAULT now()
);

CREATE TABLE raw.transactions (
    transaction_id      TEXT,
    user_id             TEXT,
    amount              NUMERIC(12,2),
    currency            TEXT,
    merchant_id         TEXT,
    merchant_category   TEXT,
    transaction_country TEXT,
    device_id           TEXT,
    transaction_ts      TIMESTAMP,
    ingestion_ts        TIMESTAMP DEFAULT now()
);


-- core 
CREATE TABLE core.users (
    user_id           TEXT PRIMARY KEY,
    registration_date DATE,
    home_country      TEXT,
    risk_segment      TEXT,
    created_at        TIMESTAMP DEFAULT now()
);

CREATE TABLE core.devices (
    device_id       TEXT PRIMARY KEY,
    device_type     TEXT,
    first_seen_ts   TIMESTAMP
);

CREATE TABLE core.merchants (
    merchant_id       TEXT PRIMARY KEY,
    merchant_category TEXT
);

CREATE TABLE core.transactions (
    transaction_id      TEXT PRIMARY KEY,
    user_id             TEXT REFERENCES core.users(user_id),
    device_id           TEXT REFERENCES core.devices(device_id),
    merchant_id         TEXT REFERENCES core.merchants(merchant_id),
    amount              NUMERIC(12,2),
    currency            TEXT,
    transaction_country TEXT,
    transaction_ts      TIMESTAMP,
    is_fraud            BOOLEAN
);


-- features
CREATE TABLE features.transaction_features_1h (
    transaction_id       TEXT PRIMARY KEY,
    tx_count_last_1h     INTEGER,
    avg_amount_last_1h   NUMERIC(12,2),
    is_new_device        BOOLEAN,
    is_foreign_tx        BOOLEAN,
    calculated_at        TIMESTAMP
);

CREATE TABLE features.transaction_features_24h (
    transaction_id       TEXT PRIMARY KEY,
    tx_count_last_24h    INTEGER,
    avg_amount_last_24h  NUMERIC(12,2),
    calculated_at        TIMESTAMP
);

CREATE TABLE features.user_behavior_features (
    user_id              TEXT,
    tx_count_7d          INTEGER,
    avg_amount_7d        NUMERIC(12,2),
    distinct_countries_7d INTEGER,
    calculated_at        TIMESTAMP,
    PRIMARY KEY (user_id, calculated_at)
);


-- mart 
CREATE TABLE mart.fraud_predictions (
    transaction_id     TEXT PRIMARY KEY,
    fraud_probability  NUMERIC(5,4),
    model_version      TEXT,
    prediction_ts      TIMESTAMP
);

CREATE TABLE mart.model_metrics_daily (
    metric_date   DATE,
    model_version TEXT,
    auc           NUMERIC(5,4),
    precision     NUMERIC(5,4),
    recall        NUMERIC(5,4),
    PRIMARY KEY (metric_date, model_version)
);


-- meta
CREATE TABLE meta.feature_registry (
    feature_name     TEXT PRIMARY KEY,
    schema_name      TEXT,
    table_name       TEXT,
    description      TEXT,
    aggregation_win  TEXT,        -- 1h / 24h / 7d
    is_active        BOOLEAN,
    created_at       TIMESTAMP DEFAULT now()
);

CREATE TABLE meta.model_registry (
    model_name     TEXT,
    model_version  TEXT,
    description    TEXT,
    trained_at     TIMESTAMP,
    is_active      BOOLEAN,
    PRIMARY KEY (model_name, model_version)
);



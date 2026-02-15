# **ETL Layer – Fraud Detection System**

## **1. Overview**
The ETL layer transforms synthetic raw transactional data into structured business entities and analytical features used for fraud detection.

The pipeline follows a production-like multi-schema architecture:

```nginx
raw → core → features → mart
```

This design mirrors real-world antifraud data platforms and supports:
- Clear separation of concerns
- Idempotent SQL transformations
- SQL-centric feature engineering
- Near-real-time fraud scoring simulation

PostgreSQL 18 is used as:
- OLTP store (raw + core)
- Lightweight feature store
- Analytical mart
- Data loading is implemented using psycopg2.

## **2. Schemas and Responsibilities**

### 2.1 raw schema
**Purpose**: store append-only raw events with minimal transformation.

**Tables:**
- raw.users
- raw.transactions

**Characteristics:**
- Append-only ingestion
- Minimal business logic
- Includes ingestion_ts
- Serves as landing zone

### 2.2 core schema
Purpose: normalized business entities.

Tables:
- core.users
- core.transactions
- core.devices
- core.merchants

Characteristics:
- Business keys
- Strong typing
- Deduplication
- Clean foreign key relationships

Transformation files:
- raw_to_core_users.sql
- raw_to_core_transactions.sql

### 2.3 features schema

Purpose: SQL-based feature store for fraud rules and ML.

Tables:
- features.transaction_features_1h
- features.transaction_features_24h
- features.user_behavior_features

Characteristics:
- Time-window aggregations
- Fully SQL-driven feature computation
- Idempotent scripts
- Recomputable on demand

Feature scripts:
- core_to_transaction_feature_1h.sql
- core_to_transaction_feature_24h.sql
- core_to_user_behavior_feature.sql

### 2.4 mart schema

Purpose: analytics and fraud monitoring.

Tables:
- mart.fraud_predictions
- mart.model_metrics_daily (future extension)

mart.fraud_predictions is populated during synthetic data generation to simulate a scoring layer

## **3. Data Generation Layer (Synthetic Data)**

### Location: 
```ssh
data_generator/
```
This layer simulates a fraud domain environment.

### 3.1 Generation Flow

1. Generate users
2. Generate devices
3. Generate merchants
4. Generate transactions
5. Apply rule-based fraud labeling
6. Insert into database

Run only data generation:
```ssh
python -m data_generator.run
```

Run full pipeline:
```ssh
python scripts/run_pipeline.py
```

## **4. data_generator.run – Raw Data Loader**

### This module:

- Generates synthetic data
- Validates fraud rate
- Writes to PostgreSQL

### 4.1 Generator Modes

Controlled via environment variable:
```python
$env:GENERATOR_MODE="DEV"
$env:GENERATOR_MODE="INCREMENTAL"
```

Mode
- DEV - Truncates raw tables before insert
- INCREMENTAL - Appends new data

DEV mode resets:
```SQL
TRUNCATE raw.transactions, raw.users CASCADE;
```

### 4.2 Insert Logic
**Users** → raw.users

Bulk insert using executemany.

**Devices** → core.devices

Upsert logic:
```SQL
ON CONFLICT (device_id) DO NOTHING
```

**Merchants** → core.merchants

Upsert logic:
```SQL
ON CONFLICT (merchant_id) DO NOTHING
```

**Transactions** → raw.transactions

Includes:
- transaction_ts (event time)
- ingestion_ts (system time)


**Fraud Predictions** → mart.fraud_predictions

Simulated scoring output:
- fraud_probability
- is_fraud
- model_version = 'rules_v1'
- score_source = 'rules'
- prediction_ts

### 4.3 Fraud Rate Validation
After labeling:
```python
fraud_rate = sum(tx[-2] for tx in labeled_transactions) / len(labeled_transactions)
assert 0.0 <= fraud_rate <= 0.05
```
This enforces realistic fraud distribution (≤ 5%).


## **5. scripts/run_pipeline.py – Full ETL Orchestration**
This script orchestrates the entire pipeline.

### Step 1 – Generate Raw Data
```python
subprocess.run([sys.executable, "-m", "data_generator.run"], check=True)
```

### Step 2 – Raw → Core Transformations

- raw_to_core_users.sql
- raw_to_core_transactions.sql

### Step 3 – Feature Engineering

- core_to_transaction_feature_1h.sql
- core_to_transaction_feature_24h.sql
- core_to_user_behavior_feature.sql

Execution pattern:
```python
cur.execute(sql)
conn.commit()
```

All SQL scripts are:
- Transactional
- Idempotent
- Fail-fast (rollback on error)

## **6. Feature Engineering Strategy**

The project intentionally uses SQL-centric feature computation.

### **Rationale**

- Simulates production antifraud systems
- Avoids Python/feature-store drift
- Ensures consistency between:
- Rule-based scoring
- ML scoring
- Monitoring

### **Example Features**

- tx_count_last_1h
- tx_count_last_24h
- avg_amount_last_7d
- is_new_device
- is_foreign_transaction

Window functions and time-based aggregations are implemented directly in SQL.

## **7. Idempotency and Safety**

### Design principles:

- ON CONFLICT DO NOTHING for dimensions
- Transactional SQL execution
- Explicit commit/rollback handling
- Fraud rate validation
- Controlled DEV reset mode

This mirrors production-grade data warehouse patterns.

## **8. End-to-End Pipeline Flow**
```pgsql
1. Synthetic generation (Python)
2. Insert into raw schema
3. Normalize into core schema
4. Compute time-window features
5. Store fraud predictions in mart
```

### This simulates:

- Event ingestio
- Batch transformation
- Feature store computation
- Fraud scoring layer
- Analytical monitoring

## **9. Technology Stack**

- PostgreSQL 18
- psycopg2
- SQL (window functions, aggregations)
- Python 3.11+
- Subprocess-based orchestration


## **10. Design Principles Demonstrated**
### This ETL layer demonstrates:

- Multi-schema data warehouse design
- SQL-first feature engineering
- Reproducible synthetic fraud environment
- Transaction-safe loading
- Clear orchestration separation
- Production-style architecture

## **11. Future Improvements**
### Potential enhancements:

- Incremental feature computation (CDC-based)
- Materialized views for heavy aggregations
- Airflow/Prefect orchestration
- Partitioning by transaction_ts
- Index optimization
-Data quality validation layer
# ETL Data Pipeline

[![CI](https://github.com/your-username/etl-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/your-username/etl-data-pipeline/actions)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://python.org)
[![dbt](https://img.shields.io/badge/dbt-1.7-orange.svg)](https://getdbt.com)
[![Airflow](https://img.shields.io/badge/Airflow-2.8-green.svg)](https://airflow.apache.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

An automated, production-grade multi-source ETL pipeline implementing the **Medallion Architecture** (Bronze → Silver → Gold) with Apache Airflow orchestration, dbt transformations, and full data lineage tracking.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Data Sources                                    │
│  ┌──────────────────────┐  ┌────────────────────┐  ┌──────────────────┐ │
│  │ Open Exchange Rates  │  │  REST Countries     │  │   CSV Dataset    │ │
│  │ API (daily rates)    │  │  API (weekly meta)  │  │  (static file)   │ │
│  └──────────┬───────────┘  └────────┬───────────┘  └────────┬─────────┘ │
└─────────────┼────────────────────────┼──────────────────────┼────────────┘
              │                        │                       │
              ▼                        ▼                       ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     Python Extractor Layer                               │
│       BaseExtractor (ABC) → ApiExtractor / CsvExtractor                 │
│       Retry logic · Exponential backoff · Metadata enrichment           │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   Airflow Orchestration (3 DAGs)                         │
│   extract_currencies (daily)  →  run_dbt_transforms (daily)             │
│   extract_countries  (weekly)    ExternalTaskSensor dependency           │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼               ▼
             ┌──────────┐  ┌──────────┐  ┌──────────────┐
             │  🥉 BRONZE │  │ 🥈 SILVER │  │   🥇 GOLD    │
             │  (Raw)    │  │(Cleaned)  │  │(Aggregated)  │
             │           │  │           │  │              │
             │raw_curr.  │→ │fct_rates  │→ │agg_summary   │
             │raw_count. │  │dim_country│  │agg_by_region │
             │           │  │           │  │agg_country_  │
             │           │  │           │  │  currency_map│
             └──────────┘  └──────────┘  └──────┬───────┘
                                                  │
                                                  ▼
                                    ┌─────────────────────┐
                                    │  📊 Dashboard        │
                                    │  (Static HTML +      │
                                    │   Chart.js)          │
                                    └─────────────────────┘
```

## Medallion Architecture

This pipeline implements the **Bronze / Silver / Gold medallion architecture** — a data lakehouse pattern that provides clear separation of concerns across data quality tiers.

| Layer | Schema | Materialization | Purpose |
|-------|--------|-----------------|---------|
| 🥉 Bronze | `bronze` | Table (raw) + dbt views | Immutable raw data, exactly as received from source |
| 🥈 Silver | `silver` | dbt Tables | Cleaned, typed, deduplicated, joined |
| 🥇 Gold | `gold` | dbt Tables | Aggregated, business-ready, powers dashboards |

**Why this matters:** Each layer is independently queryable, making debugging straightforward — if a Gold metric looks wrong, you can trace it back through Silver to Bronze raw data.

---

## Features

- **Multi-source ingestion** — REST APIs + CSV files via a unified extractor hierarchy
- **Medallion architecture** — Bronze (raw) -> Silver (clean) → Gold (aggregated)
- **Data lineage tracking** — every record carries `_source`, `_extracted_at`, `_pipeline_run_id`; all runs logged to `bronze.pipeline_runs`
- **Data quality checks** — dbt tests: `not_null`, `unique`, `accepted_values` on every model
- **Incremental loading strategy** — upsert on natural keys prevents duplicates; re-running any DAG is safe
- **Retry with exponential backoff** — via `tenacity`; transparent to callers
- **Idempotent DAGs** — `catchup=False`, upsert loads, `max_active_runs=1`
- **CI/CD** — GitHub Actions runs lint, unit tests, dbt compile + test on every PR

---

## Tech Stack

**Python** · **Apache Airflow 2.8** · **dbt-core 1.7** · **PostgreSQL 15** · **Pandas** · **Docker** · **GitHub Actions**

---

## Project Structure

```
etl-data-pipeline/
├── dags/
│   ├── extract_currencies.py      # Daily: Open Exchange Rates → bronze
│   ├── extract_countries.py       # Weekly: REST Countries → bronze
│   ├── run_dbt_transforms.py      # Daily: dbt silver + gold + tests
│   └── utils/
│       └── loader.py              # PostgreSQL upsert + audit logging
├── extractors/
│   ├── base_extractor.py          # Abstract base: retry, metadata, logging
│   ├── api_extractor.py           # HTTP: session pooling, error handling
│   ├── csv_extractor.py           # CSV: auto-detect delimiter, type casting
│   ├── open_exchange_rates_extractor.py
│   └── rest_countries_extractor.py
├── dbt_project/
│   ├── models/
│   │   ├── bronze/                # stg_currencies, stg_countries (views)
│   │   ├── silver/                # dim_countries, fct_exchange_rates
│   │   └── gold/                  # agg_currency_summary, agg_rates_by_region
│   ├── tests/                     # Custom dbt singular tests
│   ├── dbt_project.yml
│   └── profiles.yml
├── dashboard/
│   └── index.html                 # Static HTML + Chart.js dashboard
├── tests/
│   └── test_extractors.py         # Unit tests (pytest + mocks)
├── scripts/
│   └── init_db.sql                # Schema + audit table creation
├── .github/workflows/ci.yml       # GitHub Actions: lint + test + dbt
├── docker-compose.yml             # PostgreSQL + Airflow
├── requirements.txt
└── .env.example
```

---

## Quick Start

### Prerequisites

- Docker Desktop
- Git
- A free [Open Exchange Rates API key](https://openexchangerates.org/signup/free)

### 1. Clone and configure

```bash
git clone https://github.com/your-username/etl-data-pipeline.git
cd etl-data-pipeline

cp .env.example .env
```

Edit `.env` and fill in:
```env
OPEN_EXCHANGE_RATES_APP_ID=your_real_key_here
AIRFLOW_FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
```

### 2. Start the stack

```bash
docker-compose up -d
```

Wait ~60 seconds for Airflow to initialize, then open:

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| PostgreSQL | localhost:5432 | see `.env` |
| Dashboard | Open `dashboard/index.html` in browser | — |

### 3. Trigger the pipeline

In the Airflow UI:
1. Enable **`extract_currencies`** -> trigger manually
2. Enable **`extract_countries`** -> trigger manually
3. Enable **`run_dbt_transforms`** -> triggers automatically after currencies DAG completes

Or via CLI:
```bash
docker exec etl_airflow_scheduler airflow dags trigger extract_currencies
docker exec etl_airflow_scheduler airflow dags trigger extract_countries
```

### 4. Run tests locally

```bash
pip install -r requirements.txt
pytest tests/ -v
```

---

## Data Lineage

Every record loaded into Bronze carries three lineage fields:

| Field | Example | Purpose |
|-------|---------|---------|
| `_source` | `open_exchange_rates` | Which system produced this data |
| `_extracted_at` | `2024-03-15T02:14:33Z` | When the extractor ran |
| `_pipeline_run_id` | `manual__2024-03-15T02:00:00+00:00` | Airflow run_id — links to DAG execution |

All runs are also persisted in `bronze.pipeline_runs`:

```sql
SELECT source_name, status, records_loaded, started_at, finished_at
FROM bronze.pipeline_runs
ORDER BY started_at DESC
LIMIT 10;
```

---

## Data Quality

dbt tests run after every transformation:

```yaml
# Example from silver/schema.yml
- name: cca2
  tests:
    - not_null
    - unique

- name: region
  tests:
    - accepted_values:
        values: [Africa, Americas, Antarctic, Asia, Europe, Oceania]
```

The CI pipeline blocks merges if any dbt test fails.

---

## Incremental Loading Strategy

The pipeline uses **upsert (INSERT ... ON CONFLICT DO UPDATE)** as its incremental strategy:

- **Currencies**: conflict key = `(target_currency, rate_date)` — one row per currency per day
- **Countries**: conflict key = `cca2` — one row per country, always updated to latest

This means:
- **Re-running a DAG for the same date** is safe — no duplicates created
- **Backfilling** works correctly — older data isn't overwritten by newer
- **Late arrivals** (data arriving after initial load) update the existing row

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `POSTGRES_USER` | YES | PostgreSQL username |
| `POSTGRES_PASSWORD` | YES | PostgreSQL password |
| `POSTGRES_DB` | YES | Main ETL database name |
| `OPEN_EXCHANGE_RATES_APP_ID` | YES | Free API key from openexchangerates.org |
| `AIRFLOW_FERNET_KEY` | YES | Encryption key for Airflow secrets |
| `AIRFLOW_USER` | YES | Airflow web UI username |
| `AIRFLOW_PASSWORD` | YES | Airflow web UI password |

---

## dbt Commands Reference

```bash
# Navigate to dbt project
cd dbt_project

# Run all models
dbt run

# Run only silver layer
dbt run --select silver

# Run tests
dbt test

# Run tests for a specific model
dbt test --select dim_countries

# Generate + serve documentation
dbt docs generate
dbt docs serve

# Check lineage graph
dbt ls --select +agg_currency_summary  # show all upstream dependencies
```

---

## License

MIT — see [LICENSE](LICENSE).
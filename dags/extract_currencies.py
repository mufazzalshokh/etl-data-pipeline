"""
Airflow DAG: Daily currency rate extraction from Open Exchange Rates API.

Schedule: Daily at 02:00 UTC (after rates are published for the previous day)
Produces: bronze.raw_currencies

Pipeline:
  start → extract_rates → load_to_bronze → end

Key design decisions:
  - Idempotent: re-running the same logical_date produces the same result
    because we upsert on (target_currency, rate_date).
  - Audit logging: every run writes to bronze.pipeline_runs for lineage.
  - XCom: records_count is pushed to XCom so downstream DAGs can check
    if this run produced data before attempting dbt transforms.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Make extractors importable inside Airflow workers
sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

# DAG default args — applied to all tasks unless overridden

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,           # don't block on prior run failures
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

# Task functions


def extract_and_load_currencies(
    *,
    dag_run=None,
    ti=None,
    **kwargs,
) -> int:
    """
    Extract currency rates and load them into bronze.raw_currencies.

    This function is intentionally a single task doing both extract + load
    to keep the graph minimal for a single-source pipeline. For multi-step
    sources with heavy transforms, split into separate tasks.

    Args:
        dag_run: Injected by Airflow — provides run_id for lineage tracking.
        ti:      Task instance — used to push record count to XCom.

    Returns:
        Number of records loaded (also pushed to XCom as "records_loaded").
    """
    from extractors import OpenExchangeRatesExtractor
    from dags.utils.loader import (
        load_records_to_bronze,
        log_run_start,
        log_run_finish,
    )

    run_id = dag_run.run_id if dag_run else "local_test"
    dag_id = "extract_currencies"

    # ── 1. Audit: mark run as started ──────────────────────────────────────
    audit_id = log_run_start(run_id=run_id, dag_id=dag_id, source_name="open_exchange_rates")

    try:
        # ── 2. Extract ─────────────────────────────────────────────────────
        extractor = OpenExchangeRatesExtractor()
        records = extractor.extract_with_metadata(pipeline_run_id=run_id)

        # ── 3. Load to Bronze ──────────────────────────────────────────────
        # Upsert key: one record per currency per day
        loaded = load_records_to_bronze(
            records=records,
            table_name="raw_currencies",
            conflict_columns=["target_currency", "rate_date"],
        )

        # ── 4. Push count to XCom for downstream tasks ─────────────────────
        if ti:
            ti.xcom_push(key="records_loaded", value=loaded)

        # ── 5. Audit: mark run as succeeded ────────────────────────────────
        log_run_finish(
            audit_id=audit_id,
            status="success",
            records_extracted=len(records),
            records_loaded=loaded,
        )

        logger.info(
            "Currency extraction complete | run_id=%s | loaded=%d",
            run_id, loaded,
        )
        return loaded

    except Exception as exc:
        log_run_finish(
            audit_id=audit_id,
            status="failed",
            error_message=str(exc),
        )
        raise  # re-raise so Airflow marks the task as failed


# DAG definition

with DAG(
    dag_id="extract_currencies",
    description="Daily ingestion of currency exchange rates → bronze.raw_currencies",
    schedule_interval="0 2 * * *",   # 02:00 UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,                   # don't backfill historical runs
    max_active_runs=1,               # prevent concurrent runs on the same source
    tags=["bronze", "currencies", "open-exchange-rates"],
    default_args=DEFAULT_ARGS,
    doc_md="""
## extract_currencies

Fetches latest currency exchange rates from [Open Exchange Rates](https://openexchangerates.org/)
and loads them into the **Bronze layer** (`bronze.raw_currencies`).

### Schedule
Daily at 02:00 UTC.

### Idempotency
Re-running produces the same result — upsert on `(target_currency, rate_date)`.

### Lineage
Every run is logged in `bronze.pipeline_runs` with row counts and status.
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    extract_and_load = PythonOperator(
        task_id="extract_and_load_currencies",
        python_callable=extract_and_load_currencies,
    )

    end = EmptyOperator(task_id="end")

    start >> extract_and_load >> end
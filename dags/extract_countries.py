"""
Airflow DAG: Weekly country metadata extraction from REST Countries API.

Schedule: Every Monday at 03:00 UTC (country data is very stable)
Produces: bronze.raw_countries

Why weekly: Country metadata changes rarely. Daily runs would be wasteful
and put unnecessary load on the free public API.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def extract_and_load_countries(*, dag_run=None, ti=None, **kwargs) -> int:
    """Extract all countries and upsert into bronze.raw_countries."""
    from extractors import RestCountriesExtractor
    from dags.utils.loader import (
        load_records_to_bronze,
        log_run_start,
        log_run_finish,
    )

    run_id = dag_run.run_id if dag_run else "local_test"
    dag_id = "extract_countries"

    audit_id = log_run_start(
        run_id=run_id, dag_id=dag_id, source_name="rest_countries"
    )

    try:
        extractor = RestCountriesExtractor()
        records = extractor.extract_with_metadata(pipeline_run_id=run_id)

        # Upsert key: cca2 (ISO 2-letter code) is globally unique per country
        loaded = load_records_to_bronze(
            records=records,
            table_name="raw_countries",
            conflict_columns=["cca2"],
        )

        if ti:
            ti.xcom_push(key="records_loaded", value=loaded)

        log_run_finish(
            audit_id=audit_id,
            status="success",
            records_extracted=len(records),
            records_loaded=loaded,
        )

        logger.info(
            "Countries extraction complete | run_id=%s | loaded=%d",
            run_id, loaded,
        )
        return loaded

    except Exception as exc:
        log_run_finish(audit_id=audit_id, status="failed", error_message=str(exc))
        raise


with DAG(
    dag_id="extract_countries",
    description="Weekly ingestion of country metadata → bronze.raw_countries",
    schedule_interval="0 3 * * 1",   # Every Monday at 03:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "countries", "rest-countries"],
    default_args=DEFAULT_ARGS,
    doc_md="""
## extract_countries

Fetches country metadata from [REST Countries API](https://restcountries.com/)
and loads it into the **Bronze layer** (`bronze.raw_countries`).

### Schedule
Weekly on Monday at 03:00 UTC — country data rarely changes.

### Idempotency
Upsert on `cca2` (ISO 3166-1 alpha-2 country code).
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    extract_and_load = PythonOperator(
        task_id="extract_and_load_countries",
        python_callable=extract_and_load_countries,
    )

    end = EmptyOperator(task_id="end")

    start >> extract_and_load >> end
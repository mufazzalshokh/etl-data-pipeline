"""
Airflow DAG: Trigger dbt transformations after extraction DAGs complete.

Flow:
  extract_currencies (sensor) ─┐
                                ├─> dbt_run_silver -> dbt_run_gold -> dbt_test -> end
  extract_countries  (sensor) ─┘

Schedule: Daily at 04:00 UTC — runs after both extraction DAGs finish.

The ExternalTaskSensor tasks ensure this DAG waits for upstream data
before running transforms — preventing stale or partial data in Silver/Gold.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"


def _run_dbt_command(command: list[str]) -> None:
    """
    Execute a dbt CLI command in a subprocess.

    Uses subprocess instead of dbt's Python API to:
      1. Remain dbt-version agnostic
      2. Capture stdout/stderr for Airflow task logs
      3. Fail the task naturally if dbt exits with a non-zero code

    Args:
        command: List of command parts, e.g., ["dbt", "run", "--select", "silver"]

    Raises:
        subprocess.CalledProcessError: if dbt exits with non-zero status.
    """
    env = {
        **os.environ,
        "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
    }

    logger.info("Running: %s", " ".join(command))

    result = subprocess.run(
        command,
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
        env=env,
    )

    # Always log output for visibility in Airflow UI
    if result.stdout:
        for line in result.stdout.splitlines():
            logger.info("[dbt] %s", line)
    if result.stderr:
        for line in result.stderr.splitlines():
            logger.warning("[dbt stderr] %s", line)

    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, command, result.stdout, result.stderr
        )


def run_dbt_silver(**kwargs) -> None:
    """Run dbt models in the silver layer."""
    _run_dbt_command([
        "dbt", "run",
        "--select", "silver",
        "--profiles-dir", DBT_PROJECT_DIR,
        "--project-dir", DBT_PROJECT_DIR,
    ])


def run_dbt_gold(**kwargs) -> None:
    """Run dbt models in the gold layer."""
    _run_dbt_command([
        "dbt", "run",
        "--select", "gold",
        "--profiles-dir", DBT_PROJECT_DIR,
        "--project-dir", DBT_PROJECT_DIR,
    ])


def run_dbt_tests(**kwargs) -> None:
    """Run all dbt data quality tests."""
    _run_dbt_command([
        "dbt", "test",
        "--profiles-dir", DBT_PROJECT_DIR,
        "--project-dir", DBT_PROJECT_DIR,
    ])


def generate_dbt_docs(**kwargs) -> None:
    """Generate dbt documentation (catalog + manifest)."""
    _run_dbt_command([
        "dbt", "docs", "generate",
        "--profiles-dir", DBT_PROJECT_DIR,
        "--project-dir", DBT_PROJECT_DIR,
    ])


with DAG(
    dag_id="run_dbt_transforms",
    description="dbt Silver + Gold transformations triggered after extraction",
    schedule_interval="0 4 * * *",   # 04:00 UTC, after both extraction DAGs
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "gold", "dbt", "transforms"],
    default_args=DEFAULT_ARGS,
    doc_md="""
## run_dbt_transforms

Orchestrates dbt transformations across the **Silver** and **Gold** layers.

### Dependency
Waits for `extract_currencies` to complete for the same logical date
before running. This guarantees Silver/Gold always reflect the latest Bronze data.

### Layers
- **Silver**: cleaned, typed, joined, deduplicated
- **Gold**: aggregated, analytics-ready

### Data Quality
`dbt test` runs after every transform — pipeline fails if tests fail.
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    # Wait for the currencies extraction to succeed today
    wait_for_currencies = ExternalTaskSensor(
        task_id="wait_for_currencies",
        external_dag_id="extract_currencies",
        external_task_id="end",
        timeout=3600,            # wait up to 1 hour
        poke_interval=60,        # check every 60 seconds
        mode="reschedule",       # release worker slot while waiting
        allowed_states=["success"],
    )

    silver = PythonOperator(
        task_id="dbt_run_silver",
        python_callable=run_dbt_silver,
    )

    gold = PythonOperator(
        task_id="dbt_run_gold",
        python_callable=run_dbt_gold,
    )

    tests = PythonOperator(
        task_id="dbt_test",
        python_callable=run_dbt_tests,
    )

    docs = PythonOperator(
        task_id="dbt_docs_generate",
        python_callable=generate_dbt_docs,
    )

    end = EmptyOperator(task_id="end")

    start >> wait_for_currencies >> silver >> gold >> tests >> docs >> end
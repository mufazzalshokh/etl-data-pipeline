"""
PostgreSQL loader utility — writes enriched records to the Bronze layer.

Responsibilities:
  - Creates the target table if it doesn't exist (schema-on-write)
  - Uses INSERT ... ON CONFLICT DO UPDATE (upsert) for idempotent loads
  - Logs pipeline run start/finish to bronze.pipeline_runs for data lineage
  - Handles type coercion between Python dicts and PostgreSQL columns
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


# Connection helper functions and context manager for PostgreSQL connections

def _get_conn_params() -> dict[str, Any]:
    return {
        "host": os.environ["POSTGRES_HOST"],
        "port": int(os.environ.get("POSTGRES_PORT", 5432)),
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "dbname": os.environ["POSTGRES_DB"],
    }


@contextmanager
def get_connection():
    """Context manager for a psycopg2 connection — auto-commits or rolls back."""
    conn = psycopg2.connect(**_get_conn_params())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def log_run_start(
    run_id: str,
    dag_id: str,
    source_name: str,
) -> int:
    """Insert a pipeline_runs row and return its ID."""
    sql = """
        INSERT INTO bronze.pipeline_runs
            (run_id, dag_id, source_name, status, started_at)
        VALUES (%(run_id)s, %(dag_id)s, %(source_name)s, 'running', NOW())
        RETURNING id
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {
                "run_id": run_id,
                "dag_id": dag_id,
                "source_name": source_name,
            })
            return cur.fetchone()[0]


def log_run_finish(
    audit_id: int,
    status: str,
    records_extracted: int = 0,
    records_loaded: int = 0,
    error_message: str | None = None,
) -> None:
    """Update the pipeline_runs row with outcome."""
    sql = """
        UPDATE bronze.pipeline_runs
        SET
            status             = %(status)s,
            records_extracted  = %(records_extracted)s,
            records_loaded     = %(records_loaded)s,
            finished_at        = NOW(),
            error_message      = %(error_message)s
        WHERE id = %(id)s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {
                "id": audit_id,
                "status": status,
                "records_extracted": records_extracted,
                "records_loaded": records_loaded,
                "error_message": error_message,
            })


def load_records_to_bronze(
    records: list[dict[str, Any]],
    table_name: str,
    conflict_columns: list[str],
) -> int:
    """
    Upsert a list of records into a Bronze-layer PostgreSQL table.

    The table is created dynamically based on the keys of the first record.
    All metadata fields (_source, _extracted_at, _pipeline_run_id) are stored
    as standard columns, making them queryable for lineage analysis.

    Args:
        records:          Enriched records from extractor.extract_with_metadata()
        table_name:       Target table name (without schema, e.g., "raw_currencies")
                          Will be created in the `bronze` schema.
        conflict_columns: Column(s) that identify a unique record for upsert.
                          E.g., ["target_currency", "rate_date"]

    Returns:
        Number of rows upserted.

    Example:
        load_records_to_bronze(
            records=enriched_records,
            table_name="raw_currencies",
            conflict_columns=["target_currency", "rate_date"],
        )
    """
    if not records:
        logger.warning("load_records_to_bronze called with 0 records — skipping")
        return 0

    full_table = f"bronze.{table_name}"
    columns = list(records[0].keys())

    _ensure_table_exists(full_table, columns, conflict_columns)

    loaded = _upsert_batch(full_table, columns, records, conflict_columns)
    logger.info("Loaded %d rows into %s", loaded, full_table)
    return loaded


def _ensure_table_exists(
    full_table: str,
    columns: list[str],
    conflict_columns: list[str],
) -> None:
    """
    CREATE TABLE IF NOT EXISTS with TEXT columns for all fields.
    Uses a composite unique constraint on conflict_columns for upserts.
    """
    col_defs = ",\n    ".join(
        f"{col} TEXT" for col in columns
        if col not in ("_source", "_extracted_at", "_pipeline_run_id")
    )

    # Always add lineage columns with explicit types
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            id               SERIAL PRIMARY KEY,
            {col_defs},
            _source          TEXT,
            _extracted_at    TIMESTAMPTZ,
            _pipeline_run_id TEXT,
            _loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE ({", ".join(conflict_columns)})
        )
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)


def _upsert_batch(
    full_table: str,
    columns: list[str],
    records: list[dict[str, Any]],
    conflict_columns: list[str],
) -> int:
    """
    INSERT ... ON CONFLICT DO UPDATE SET ... for all non-key columns.
    Uses execute_values for batched performance.
    """
    non_conflict_cols = [c for c in columns if c not in conflict_columns]

    # Build: INSERT INTO tbl (col1, col2, ...) VALUES %s
    #        ON CONFLICT (key1, key2) DO UPDATE SET col3 = EXCLUDED.col3 ...
    update_set = ", ".join(
        f"{col} = EXCLUDED.{col}" for col in non_conflict_cols
    )
    conflict_target = ", ".join(conflict_columns)

    insert_sql = f"""
        INSERT INTO {full_table} ({", ".join(columns)})
        VALUES %s
        ON CONFLICT ({conflict_target})
        DO UPDATE SET {update_set}, _loaded_at = NOW()
    """

    # Serialize values in column order; cast None to NULL
    rows = [
        tuple(
            str(record.get(col)) if record.get(col) is not None else None
            for col in columns
        )
        for record in records
    ]

    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=500)
            return cur.rowcount

-- init_db.sql
-- Runs once on first PostgreSQL container startup.
-- Creates the Airflow metadata DB and the ETL warehouse schemas.
-- ============================================================

-- Create Airflow's separate metadata database
CREATE DATABASE airflow_metadata;

-- Schema-creation statements below run against whichever database this
-- script was invoked against — the official Postgres Docker image already
-- connects init scripts to the POSTGRES_DB-named database automatically,
-- and CI invokes psql with an explicit -d flag. A hardcoded \connect here
-- previously assumed the database was always named "etl_pipeline", which
-- broke in CI (where it's "test_etl") and in any local setup using a
-- different POSTGRES_DB value.

-- Bronze layer: raw ingested data, immutable
CREATE SCHEMA IF NOT EXISTS bronze;

-- Silver layer: cleaned, typed, deduplicated data
CREATE SCHEMA IF NOT EXISTS silver;

-- Gold layer: aggregated, analytics-ready data
CREATE SCHEMA IF NOT EXISTS gold;

-- Pipeline audit / lineage tracking table
CREATE TABLE IF NOT EXISTS bronze.pipeline_runs (
    id            SERIAL PRIMARY KEY,
    run_id        VARCHAR(64)  NOT NULL,
    dag_id        VARCHAR(128) NOT NULL,
    source_name   VARCHAR(64)  NOT NULL,
    status        VARCHAR(16)  NOT NULL DEFAULT 'running',
    records_extracted INTEGER,
    records_loaded    INTEGER,
    started_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at   TIMESTAMPTZ,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_run_id   ON bronze.pipeline_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag_id   ON bronze.pipeline_runs(dag_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started  ON bronze.pipeline_runs(started_at);

COMMENT ON TABLE  bronze.pipeline_runs IS 'Audit log for every ETL pipeline execution — data lineage tracking';
COMMENT ON SCHEMA bronze IS 'Raw ingested data. Never modified after load.';
COMMENT ON SCHEMA silver IS 'Cleaned, typed, deduplicated data from Bronze.';
COMMENT ON SCHEMA gold   IS 'Aggregated, analytics-ready data from Silver.';

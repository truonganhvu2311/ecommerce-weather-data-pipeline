"""Day 8.7.10.12: SAFE runtime preflight for FUTURE automatic Airflow ingestion.

READ ONLY against PostgreSQL, BigQuery and GCS. Creates/deletes one temporary file
in an isolated batch runtime mount to verify filesystem write access; no ingestion,
no dbt build, no commit. Does not use legacy state/watermark.json.

The runtime paths below correspond to two bind mounts of the SAME host folder:
    /opt/airflow/de_project/data/airflow_batches  (extractor's fixed output path)
    /opt/airflow/de_project/airflow_batches_auto (future auto DAG's input path)
Keep the old /opt/airflow/de_project/airflow_batches read-only mount unchanged.
"""
from __future__ import annotations

import importlib.util
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

PROJECT = "weather-data-project-512207"
REGION = "asia-southeast1"
LATEST_COMMITTED_BATCH = "day8_customer_c990_update_20261107"
TABLES = {"customers", "products", "orders", "order_items"}
BASE = Path("/opt/airflow/de_project")
# Old/manual mounts stay untouched.
AUTO_FROM_EXTRACTOR = BASE / "data" / "airflow_batches"
AUTO_FOR_PIPELINE = BASE / "airflow_batches_auto"
LOCAL_VERIFIED_CHECKPOINT = BASE / "committed_watermarks_verified.json"
EXTRACTOR = BASE / "src" / "ingestion" / "extract_incremental_airflow.py"
UPLOADER = BASE / "src" / "loading" / "upload_airflow_batch_gcs.py"
LOADER = BASE / "src" / "loading" / "load_airflow_batch_bigquery.py"
FINALIZER = BASE / "src" / "loading" / "finalize_airflow_batch.py"


def utc_timestamp(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Checkpoint timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)


@dag(
    dag_id="day8_auto_runtime_preflight",
    description="Read-only PG/BQ preflight and temporary writable-runtime probe; no cloud writes",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["day8", "preflight", "safe", "automation"],
)
def day8_auto_runtime_preflight():
    @task(retries=0)
    def verify_worker_and_writable_batch_folder():
        required = (EXTRACTOR, UPLOADER, LOADER, FINALIZER, LOCAL_VERIFIED_CHECKPOINT)
        absent = [str(p) for p in required if not p.is_file()]
        if absent:
            raise FileNotFoundError(f"Required previously-tested scripts/checkpoint absent: {absent}")
        if not Path(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/missing/adc")).is_file():
            raise RuntimeError("Google ADC path not mounted in Airflow Worker")
        missing_python = [x for x in ("psycopg2", "dotenv", "google.cloud.bigquery")
                          if importlib.util.find_spec(x) is None]
        if missing_python:
            raise RuntimeError(
                f"Extractor prerequisites missing from AIRFLOW WORKER Python: {missing_python}. "
                "Do not install packages interactively inside a running container; "
                "extend Dockerfile.gcp.dbt and rebuild the worker image instead."
            )
        for d in (AUTO_FROM_EXTRACTOR, AUTO_FOR_PIPELINE):
            if not d.is_dir():
                raise FileNotFoundError(f"Writable runtime bind mount missing: {d}")
        # Temporary probe only. Must be visible through both mount points.
        probe = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", prefix=".day8_runtime_probe_",
                dir=AUTO_FROM_EXTRACTOR, delete=False,
            ) as fh:
                probe = Path(fh.name)
                fh.write("DAY8_RUNTIME_PROBE_ONLY\n")
            mirrored = AUTO_FOR_PIPELINE / probe.name
            if mirrored.read_text(encoding="utf-8") != "DAY8_RUNTIME_PROBE_ONLY\n":
                raise RuntimeError("Two bind mounts do not refer to the SAME host directory")
            print("WORKER DEPENDENCIES + TWO WRITABLE BATCH MOUNTS PASS")
        finally:
            if probe is not None:
                probe.unlink(missing_ok=True)
        print("Probe file removed. Existing batch directories untouched.")

    @task(retries=0)
    def verify_postgres_readonly():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        connection = PostgresHook(postgres_conn_id="postgres_ecommerce_source").get_conn()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
                cursor.execute("SELECT current_database()")
                db_name = cursor.fetchone()[0]
                if db_name != "ecommerce_source":
                    raise RuntimeError(f"Unexpected PostgreSQL source database: {db_name}")
                cursor.execute("SELECT COUNT(*) FROM public.customers WHERE customer_id = %s", ("C990",))
                count = cursor.fetchone()[0]
                if count != 1:
                    raise RuntimeError(f"Expected one PostgreSQL customer C990, got {count}")
            connection.rollback()  # No database mutations.
        finally:
            connection.close()
        print("POSTGRES SOURCE READ-ONLY PASS: ecommerce_source, C990 present")

    @task(retries=0)
    def verify_live_checkpoint_readonly():
        from google.cloud import bigquery

        doc = json.loads(LOCAL_VERIFIED_CHECKPOINT.read_text(encoding="utf-8"))
        if doc.get("source") != "de_etl.watermarks" or doc.get("verified") is not True:
            raise RuntimeError("Local checkpoint is not an explicitly verified BQ export")
        local = doc.get("committed_watermarks", {})
        if not isinstance(local, dict) or set(local) != TABLES:
            raise RuntimeError("Expected exactly 4 local source checkpoint values")
        client = bigquery.Client(project=PROJECT, location=REGION)
        job = client.query(
            f"SELECT source_table, committed_watermark, committed_batch_id "
            f"FROM `{PROJECT}.de_etl.watermarks`", location=REGION,
        )
        rows = list(job.result())
        if len(rows) != 4 or {r.source_table for r in rows} != TABLES:
            raise RuntimeError("BigQuery watermarks missing/duplicate/extra rows")
        for r in rows:
            if (utc_timestamp(r.committed_watermark) != utc_timestamp(local[r.source_table])
                    or r.committed_batch_id != LATEST_COMMITTED_BATCH):
                raise RuntimeError(f"BigQuery/local checkpoint mismatch for {r.source_table}")
        batch_rows = list(client.query(
            f"SELECT status FROM `{PROJECT}.de_etl.pipeline_batches` "
            f"WHERE batch_id = @batch",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("batch", "STRING", LATEST_COMMITTED_BATCH),
            ]), location=REGION,
        ).result())
        if len(batch_rows) != 1 or batch_rows[0].status != "committed":
            raise RuntimeError("Latest batch is not uniquely COMMITTED in pipeline_batches")
        print("BIGQUERY CHECKPOINT READ-ONLY PASS: 4/4 tables and latest batch committed")

    @task(retries=0)
    def finish():
        print("DAY 8.7.10.12 AUTOMATION RUNTIME PREFLIGHT PASS: 4/4")
        print("No extraction, GCS upload, BigQuery write, dbt build or watermark commit")
        print("Only a temporary local write probe was created and removed")

    mounted = verify_worker_and_writable_batch_folder()
    pg = verify_postgres_readonly()
    checkpoint = verify_live_checkpoint_readonly()
    end = finish()
    # Explicit dependency: every check must pass before finish.
    [mounted, pg, checkpoint] >> end


day8_auto_runtime_preflight()

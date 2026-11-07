"""Read-only Airflow DAG for a prepared incremental transaction batch.

Requires the batch and checkpoint JSON to be mounted read-only into the worker.
No PostgreSQL extraction, GCS upload, BigQuery SQL or watermark commit occurs.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

CHECKER = "/opt/airflow/de_project/src/ingestion/validate_batch_manifest.py"
MANIFEST = "/opt/airflow/de_project/airflow_batches/day8_test_20261103/manifest.json"
CHECKPOINT = "/opt/airflow/de_project/committed_watermarks_verified.json"


@dag(
    dag_id="day8_batch_manifest_preflight",
    description="Read-only verification of a prepared incremental batch",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["day8", "ingestion", "read-only"],
)
def day8_batch_manifest_preflight():
    @task(retries=0)
    def verify_mounts():
        required = [CHECKER, MANIFEST, CHECKPOINT]
        missing = [file for file in required if not Path(file).is_file()]
        if missing:
            raise FileNotFoundError(f"Read-only batch mounts missing: {missing}")
        print("INGESTION MOUNTS PASS")
        return True

    @task(retries=0, execution_timeout=timedelta(minutes=3))
    def verify_pending_batch(mounts_ok):
        if not mounts_ok:
            raise RuntimeError("Mount check failed")
        result = subprocess.run(
            [sys.executable, CHECKER, "--manifest", MANIFEST,
             "--checkpoints-json", CHECKPOINT],
            text=True,
            capture_output=True,
            timeout=150,
            check=False,
        )
        print(result.stdout)
        if result.returncode != 0:
            raise RuntimeError(f"Batch validation failed: {result.stderr[-3000:]}")
        return "BATCH MANIFEST PREFLIGHT PASS" in result.stdout

    @task(retries=0)
    def finish(batch_passed):
        if not batch_passed:
            raise RuntimeError("Manifest was not verified")
        print("DAY 8.7.10 INGESTION PREFLIGHT PASSED (READ-ONLY)")
        print("No extraction, GCS upload, BigQuery RAW load, or watermark commit")

    finish(verify_pending_batch(verify_mounts()))


day8_batch_manifest_preflight()

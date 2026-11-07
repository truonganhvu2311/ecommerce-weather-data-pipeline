"""Airflow 3 read-only GCS uploader preflight for the previously prepared empty batch.

This DAG intentionally runs --check, NOT --upload, and never commits watermarks.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

UPLOADER = "/opt/airflow/de_project/src/loading/upload_airflow_batch_gcs.py"
MANIFEST = "/opt/airflow/de_project/airflow_batches/day8_test_20261103/manifest.json"
CHECKPOINTS = "/opt/airflow/de_project/committed_watermarks_verified.json"


@dag(
    dag_id="day8_gcs_batch_check",
    description="Read-only GCS batch uploader readiness check",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["day8", "gcs", "read-only"],
)
def day8_gcs_batch_check():
    @task(retries=0)
    def check_files():
        missing = [p for p in (UPLOADER, MANIFEST, CHECKPOINTS) if not Path(p).is_file()]
        if missing:
            raise FileNotFoundError(f"Files not mounted in worker: {missing}")
        print("GCS BATCH MOUNTS PASS")
        return True

    @task(retries=0, execution_timeout=timedelta(minutes=5))
    def gcs_check(mounts_ok: bool):
        if not mounts_ok:
            raise RuntimeError("Missing files")
        cmd = [sys.executable, UPLOADER, "--check", "--manifest", MANIFEST,
               "--checkpoints-json", CHECKPOINTS]
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=240)
        print(p.stdout)
        if p.returncode != 0:
            raise RuntimeError(f"GCS batch preflight failed: {p.stderr[-3000:]}")
        if "EMPTY_BATCH:" not in p.stdout:
            raise RuntimeError("Expected empty batch has changed; stop and review")
        return True

    @task(retries=0)
    def finish(checked: bool):
        if not checked:
            raise RuntimeError("GCS check incomplete")
        print("DAY 8.7.10.4 GCS BATCH PREFLIGHT PASSED (READ-ONLY)")
        print("EMPTY BATCH - no uploads, no BigQuery writes, no checkpoint commit")

    finish(gcs_check(check_files()))


day8_gcs_batch_check()

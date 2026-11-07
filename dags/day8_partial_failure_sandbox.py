"""Day 8.7.10.16: isolated, local-only fault-injection drill.

- Reads the latest committed AUTO batch from real BigQuery before/after.
- Runs local-only SQLite/file simulation of interrupted uploads, transactions,
  QA failure, checkpoint commit and idempotent replay.
- Does NOT write real PostgreSQL, GCS, BigQuery, dbt or checkpoint state.
- Keeps existing day8_auto_incremental_pipeline.py unchanged and unscheduled.
- Does NOT prove full real-cloud crash recovery.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task, get_current_context

PROJECT = "weather-data-project-512207"
REGION = "asia-southeast1"
TABLES = ("customers", "products", "orders", "order_items")
SRC = Path("/opt/airflow/de_project/src/testing/day8_partial_write_sandbox.py")
REPORT_DIR = Path("/opt/airflow/de_project/airflow_batches_auto/.recovery_tests")


def live_read_only_audit() -> dict:
    """Read-only verification of the most recently committed AUTO batch."""
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT, location=REGION)
    wm = list(client.query(
        f"SELECT source_table,committed_watermark,committed_batch_id "
        f"FROM `{PROJECT}.de_etl.watermarks` ORDER BY source_table",
        location=REGION,
    ).result())
    if len(wm) != 4 or {r.source_table for r in wm} != set(TABLES):
        raise RuntimeError("Expected exactly 4 distinct committed watermarks")
    batches = {r.committed_batch_id for r in wm}
    if len(batches) != 1 or not all(r.committed_watermark for r in wm):
        raise RuntimeError("Checkpoint metadata incomplete or inconsistent")
    batch = next(iter(batches))
    if not batch.startswith("auto_"):
        raise RuntimeError("Most recently committed batch is not auto_; audit before proceeding")
    q = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("batch", "STRING", batch)])
    states = list(client.query(
        f"SELECT status FROM `{PROJECT}.de_etl.pipeline_batches` WHERE batch_id=@batch",
        job_config=q, location=REGION,
    ).result())
    if len(states) != 1 or states[0].status != "committed":
        raise RuntimeError("Latest batch must have a single committed status")

    registry = list(client.query(
        f"SELECT source_file,source_type,sha256,record_count "
        f"FROM `{PROJECT}.de_etl.loaded_files` WHERE batch_id=@batch",
        job_config=q, location=REGION,
    ).result())
    expected: dict[tuple[str, str], tuple[str, int]] = {}
    for row in registry:
        key = (row.source_type, row.source_file)
        if row.source_type not in TABLES or not row.source_file or not row.sha256 or row.record_count is None:
            raise RuntimeError("Incomplete file registry")
        if key in expected:
            raise RuntimeError("Duplicate registry entry")
        expected[key] = (row.sha256, int(row.record_count))
    if not expected:
        raise RuntimeError("Most recent auto batch has no registered files")

    actual = {}
    for table in TABLES:
        groups = list(client.query(
            f"SELECT source_file,COUNT(*) AS n FROM `{PROJECT}.de_raw.{table}` "
            f"WHERE batch_id=@batch GROUP BY source_file",
            job_config=q, location=REGION,
        ).result())
        for row in groups:
            key = (table, row.source_file)
            if key in actual:
                raise RuntimeError("Duplicate RAW source group")
            actual[key] = int(row.n)
    if {key: n for key, (_, n) in expected.items()} != actual:
        raise RuntimeError("RAW rows do not match loaded_files registry")

    source_entries = sorted(
        [t, f, digest, n] for (t, f), (digest, n) in expected.items()
    )
    return {
        "batch_id": batch,
        "watermarks": {r.source_table: r.committed_watermark.isoformat() for r in wm},
        "committed_batch_ids": sorted(batches),
        "registry_files": len(expected),
        "raw_rows": sum(actual.values()),
        "registry_digest": hashlib.sha256(json.dumps(source_entries).encode()).hexdigest(),
    }


@dag(
    dag_id="day8_partial_failure_sandbox",
    description="Local file/SQLite fault injection plus REAL BigQuery read-only before/after audit",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["day8", "recovery", "sandbox", "read-only-cloud", "manual-trigger"],
)
def partial_failure_sandbox():
    @task(retries=0, execution_timeout=timedelta(minutes=10))
    def snapshot_real_cloud() -> dict:
        if not SRC.is_file():
            raise FileNotFoundError(f"Mount missing: {SRC}")
        baseline = live_read_only_audit()
        print(f"REAL CLOUD READ-ONLY BASELINE PASS: batch={baseline['batch_id']}; "
              f"registry_files={baseline['registry_files']}; raw_rows={baseline['raw_rows']}")
        return baseline

    @task(retries=0, execution_timeout=timedelta(minutes=5))
    def run_local_failure_scenarios(baseline: dict) -> dict:
        del baseline
        context = get_current_context()
        run_id = context["ti"].run_id
        suffix = hashlib.sha256(run_id.encode()).hexdigest()[:18]
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        report_path = REPORT_DIR / f"day8_recovery_sandbox_{suffix}.json"
        if report_path.exists():
            print("EXISTING SANDBOX REPORT: verifying without overwrite")
        else:
            # Only local files & SQLite used by script. No credentials passed as args.
            cmd = [sys.executable, str(SRC), "--run", "--output", str(report_path)]
            safe_env = os.environ.copy()
            # No Google Cloud or PostgreSQL secrets needed by the standalone simulation.
            for secret in ("PGPASSWORD", "GOOGLE_APPLICATION_CREDENTIALS", "AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT"):
                safe_env.pop(secret, None)
            process = subprocess.run(
                cmd, text=True, capture_output=True, encoding="utf-8", errors="replace",
                timeout=180, check=False, env=safe_env,
            )
            output = (process.stdout or "") + "\n" + (process.stderr or "")
            for line in output.splitlines()[-60:]:
                print(line)
            if process.returncode != 0:
                raise RuntimeError("Local sandbox did not pass; see error above")
        report = json.loads(report_path.read_text(encoding="utf-8"))
        if (report.get("kind") != "LOCAL_SIMULATION_ONLY"
                or report.get("scenarios_passed") != 8
                or len(report.get("checks", [])) != 8
                or report.get("batch_state") != "committed"):
            raise RuntimeError("Sandbox report missing expected 8/8 results")
        print("LOCAL FAULT-INJECTION SCENARIOS: 8/8 PASS")
        print(f"AUDIT_REPORT_PATH: {report_path}")
        return {"report": str(report_path), "checks": report["checks"]}

    @task(retries=0, execution_timeout=timedelta(minutes=10))
    def verify_real_cloud_unchanged(baseline: dict, sandbox: dict) -> dict:
        after = live_read_only_audit()
        if after != baseline:
            raise RuntimeError("Real cloud state changed during test: audit external writers")
        print("REAL BIGQUERY READ-ONLY AUDIT PASS: RAW/registry and 4 watermarks unchanged")
        return sandbox

    @task(retries=0)
    def finish(sandbox: dict) -> None:
        print("DAY 8.7.10.16 SANDBOX RECOVERY PASS: 8/8 local fault-injection cases")
        print("REAL BIGQUERY AUDIT: BEFORE == AFTER; NO CLOUD WRITES BY THIS DAG")
        print("IMPORTANT: This simulates partial writes; real GCP mid-failure recovery is NOT proven")
        print(f"REPORT: {sandbox['report']}")

    before = snapshot_real_cloud()
    simulated = run_local_failure_scenarios(before)
    finish(verify_real_cloud_unchanged(before, simulated))


partial_failure_sandbox()

"""Day 8.7.10.15: ISOLATED Airflow retry drill + read-only BigQuery audit.

- Deliberately fails first attempt of fail_once_and_retry; succeeds on retry.
- Only persistent write is a marker in local, dedicated .recovery_tests dir.
- BigQuery queries are SELECT-only. Never writes PG/GCS/BQ/dbt/watermarks.
- Does NOT test recovery from a partial GCS or BigQuery production write.
- Manual triggering only; preserves the existing end-to-end DAG unchanged.
"""
from __future__ import annotations

import hashlib
import json
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, get_current_context, task

PROJECT = "weather-data-project-512207"
REGION = "asia-southeast1"
TABLES = ("customers", "products", "orders", "order_items")
MARKER_ROOT = Path("/opt/airflow/de_project/airflow_batches_auto/.recovery_tests")


def read_live_snapshot() -> dict:
    """Audit the latest committed batch; use ONLY BigQuery SELECT queries."""
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT, location=REGION)
    rows = list(client.query(
        f"SELECT source_table, committed_watermark, committed_batch_id "
        f"FROM `{PROJECT}.de_etl.watermarks` ORDER BY source_table",
        location=REGION,
    ).result())
    if len(rows) != len(TABLES) or {r.source_table for r in rows} != set(TABLES):
        raise RuntimeError("Expected exactly four unique live watermarks")
    if any(r.committed_watermark is None or not r.committed_batch_id for r in rows):
        raise RuntimeError("Missing committed checkpoint values")
    batches = {r.committed_batch_id for r in rows}
    if len(batches) != 1:
        raise RuntimeError("Inconsistent batch IDs across four checkpoints")
    batch_id = next(iter(batches))
    if not batch_id.startswith("auto_"):
        raise RuntimeError("Latest committed batch is not an auto DAG batch; audit manually")

    qp = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("batch", "STRING", batch_id)
    ])
    states = list(client.query(
        f"SELECT status FROM `{PROJECT}.de_etl.pipeline_batches` WHERE batch_id=@batch",
        job_config=qp, location=REGION,
    ).result())
    if len(states) != 1 or states[0].status != "committed":
        raise RuntimeError("Latest auto batch has no unique committed status")

    registry = list(client.query(
        f"SELECT source_file, source_type, record_count, sha256 "
        f"FROM `{PROJECT}.de_etl.loaded_files` WHERE batch_id=@batch",
        job_config=qp, location=REGION,
    ).result())
    if not registry:
        raise RuntimeError("No registry entries for latest committed auto batch")
    expected = {}
    for row in registry:
        key = (row.source_type, row.source_file)
        if (row.source_type not in TABLES or not row.source_file
                or not row.sha256 or row.record_count is None or row.record_count <= 0):
            raise RuntimeError("Invalid registry entry")
        if key in expected:
            raise RuntimeError("Duplicate registry source_file entry")
        expected[key] = int(row.record_count)

    actual = {}
    for table in TABLES:
        raw_rows = list(client.query(
            f"SELECT source_file, COUNT(*) AS n "
            f"FROM `{PROJECT}.de_raw.{table}` "
            f"WHERE batch_id=@batch GROUP BY source_file",
            job_config=qp, location=REGION,
        ).result())
        for row in raw_rows:
            key = (table, row.source_file)
            if key in actual:
                raise RuntimeError("Duplicate RAW group")
            actual[key] = int(row.n)

    if actual != expected:
        raise RuntimeError(
            "RAW/registry mismatch: refusing recovery drill before investigating latest batch"
        )
    return {
        "batch_id": batch_id,
        "checkpoints": {r.source_table: r.committed_watermark.isoformat() for r in rows},
        "registry_files": len(expected),
        "raw_rows": sum(actual.values()),
        "registry_hash": hashlib.sha256(
            json.dumps(sorted((t, f, n) for (t, f), n in expected.items()),
                       separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
    }


@dag(
    dag_id="day8_recovery_retry_drill",
    description="Isolated fail-once/retry test; read-only BigQuery RAW/registry/watermarks",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["day8", "recovery", "read-only", "manual-trigger"],
)
def recovery_retry_drill():
    @task(retries=0, execution_timeout=timedelta(minutes=10))
    def snapshot_committed_state() -> dict:
        result = read_live_snapshot()
        print("READ-ONLY BASELINE PASS: latest auto batch is committed")
        print(f"batch={result['batch_id']}; verified_files={result['registry_files']}; "
              f"raw_rows={result['raw_rows']}; checkpoint_rows=4")
        return result

    @task(retries=1, retry_delay=timedelta(seconds=15), execution_timeout=timedelta(minutes=2))
    def fail_once_and_retry(baseline: dict) -> dict:
        run_id = get_current_context()["ti"].run_id
        token = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
        MARKER_ROOT.mkdir(parents=True, exist_ok=True)
        marker = MARKER_ROOT / f"{token}.marker"
        if not marker.exists():
            # Atomic create. Intentional failure is only *after* writing this local marker.
            with marker.open("x", encoding="utf-8") as fh:
                fh.write(token + "\n")
            print("EXPECTED FAILURE: first attempt intentionally stops. Airflow should retry once.")
            raise RuntimeError("CONTROLLED_RETRY_TEST: simulated transient task failure")
        if marker.read_text(encoding="utf-8") != token + "\n":
            raise RuntimeError("Retry marker content changed unexpectedly")
        print("RETRY SUCCESS: marker exists; task resumed without rewriting it")
        return baseline

    @task(retries=0, execution_timeout=timedelta(minutes=10))
    def verify_no_bigquery_mutations(baseline: dict) -> dict:
        after = read_live_snapshot()
        if after != baseline:
            raise RuntimeError("BigQuery snapshot changed during drill; investigate other writers")
        print("READ-ONLY RECONCILIATION PASS: 4 checkpoints unchanged; "
              "RAW and registry counts unchanged")
        return after

    @task(retries=0)
    def finish(result: dict) -> None:
        print("DAY 8.7.10.15 RECOVERY DRILL PASS: injected failure recovered on one retry")
        print(f"COMMITTED_BATCH={result['batch_id']}; FILES={result['registry_files']}; "
              f"RAW_ROWS={result['raw_rows']}")
        print("NO PostgreSQL/GCS/BigQuery/dbt/watermark writes were performed by this DAG")
        print("NOTE: This verifies Airflow retry, not actual partial-write recovery")

    finish(verify_no_bigquery_mutations(fail_once_and_retry(snapshot_committed_state())))


recovery_retry_drill()

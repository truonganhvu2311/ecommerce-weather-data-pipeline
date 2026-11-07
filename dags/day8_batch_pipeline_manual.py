"""Day 8.7.10.10: manually orchestrate an already-prepared immutable batch.

IMPORTANT:
- This DAG NEVER calls the PostgreSQL extractor. Prepare the batch on Windows.
- It DOES upload GCS objects, insert RAW+registry into BigQuery, and run dbt build.
- It checks, but DOES NOT COMMIT, watermarks. Human finalization is separate.
- Trigger with a unique batch_id and confirm_write=RUN_BATCH_WRITE.
- No schedule, no retries, max_active_runs=1. Do not run other loaders concurrently.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task, get_current_context

BASE = Path("/opt/airflow/de_project")
BATCH_DIR = BASE / "airflow_batches"
CHECKPOINT = BASE / "committed_watermarks_verified.json"
VALIDATOR = BASE / "src" / "ingestion" / "validate_batch_manifest.py"
UPLOADER = BASE / "src" / "loading" / "upload_airflow_batch_gcs.py"
LOADER = BASE / "src" / "loading" / "load_airflow_batch_bigquery.py"
FINALIZER = BASE / "src" / "loading" / "finalize_airflow_batch.py"
DBT = Path("/opt/dbt-venv/bin/dbt")
DBT_PROJECT = BASE / "dbt"
DBT_PROFILES = BASE / "profiles"
BATCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")


def call_cli(command: list[str], timeout: int, env: dict | None = None) -> str:
    """Execute only fixed local programs; do not shell-expand Airflow configuration."""
    proc = subprocess.run(
        command, capture_output=True, text=True, check=False,
        timeout=timeout, env=env,
    )
    output = (proc.stdout or "") + "\n" + (proc.stderr or "")
    print("\n".join(output.splitlines()[-75:]))
    if proc.returncode != 0:
        raise RuntimeError(
            f"Command failed with exit code {proc.returncode} (inspect preceding logs)"
        )
    return output


@dag(
    dag_id="day8_batch_pipeline_manual",
    description="Manually run a PREPARED nonempty batch through GCS, BigQuery, dbt and QA; no watermark commit",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={"batch_id": "", "confirm_write": ""},
    tags=["day8", "manual", "incremental", "bigquery-writes"],
)
def day8_batch_pipeline_manual():
    @task(retries=0)
    def validate_pending_batch() -> dict:
        ctx = get_current_context()
        conf = dict(ctx.get("params", {}))
        conf.update(dict(ctx["dag_run"].conf or {}))
        batch_id = conf.get("batch_id", "")
        confirmation = conf.get("confirm_write", "")
        if not isinstance(batch_id, str) or not BATCH_RE.fullmatch(batch_id):
            raise ValueError("Supply a valid 1-64 character batch_id in DAG Run configuration")
        if confirmation != "RUN_BATCH_WRITE":
            raise ValueError("Explicit consent required: confirm_write=RUN_BATCH_WRITE")

        manifest_path = BATCH_DIR / batch_id / "manifest.json"
        required = (
            VALIDATOR, UPLOADER, LOADER, FINALIZER, CHECKPOINT, manifest_path,
            DBT, DBT_PROJECT / "dbt_project.yml", DBT_PROFILES / "profiles.yml",
            Path(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/no-adc")),
        )
        missing = [str(p) for p in required if not p.is_file()]
        if missing:
            raise FileNotFoundError(f"Required worker mounts/runtime files absent: {missing}")

        doc = json.loads(manifest_path.read_text(encoding="utf-8"))
        if doc.get("batch_id") != batch_id or doc.get("status") != "pending":
            raise RuntimeError("Batch ID/status mismatch; only immutable pending batches allowed")
        files = doc.get("files", [])
        if not isinstance(files, list) or not files:
            raise RuntimeError("EMPTY_BATCH: do not upload or run BigQuery loader; stop this DAG")
        count = sum(int(x["record_count"]) for x in files)
        if count <= 0:
            raise RuntimeError("No records to process")

        output = call_cli(
            [sys.executable, str(VALIDATOR), "--manifest", str(manifest_path),
             "--checkpoints-json", str(CHECKPOINT)], timeout=180,
        )
        if "BATCH MANIFEST PREFLIGHT PASS" not in output:
            raise RuntimeError("Validator did not report a successful preflight")

        # Check the authoritative BigQuery checkpoint BEFORE any GCS object is created.
        from google.cloud import bigquery
        from datetime import timezone

        local_doc = json.loads(CHECKPOINT.read_text(encoding="utf-8"))
        if (local_doc.get("source") != "de_etl.watermarks"
                or local_doc.get("verified") is not True):
            raise RuntimeError("Local checkpoint export is not verified")
        mapping = local_doc.get("committed_watermarks", {})
        expected_tables = {"customers", "products", "orders", "order_items"}
        if set(mapping) != expected_tables:
            raise RuntimeError("Expected exactly four local checkpoint watermarks")
        client = bigquery.Client(project="weather-data-project-512207", location="asia-southeast1")
        sql = """SELECT source_table, committed_watermark
                 FROM `weather-data-project-512207.de_etl.watermarks`"""
        rows = list(client.query(sql, location="asia-southeast1").result())
        if len(rows) != 4 or {row.source_table for row in rows} != expected_tables:
            raise RuntimeError("Invalid live BigQuery watermarks: missing/duplicate tables")
        for row in rows:
            from datetime import datetime
            saved = datetime.fromisoformat(mapping[row.source_table].replace("Z", "+00:00"))
            if saved.tzinfo is None or row.committed_watermark is None:
                raise RuntimeError("Invalid timezone/null watermark")
            if saved.astimezone(timezone.utc) != row.committed_watermark.astimezone(timezone.utc):
                raise RuntimeError("STALE CHECKPOINT: local export does not match BigQuery")
        print("BIGQUERY LIVE CHECKPOINT PREFLIGHT PASSED (four tables)")
        print(f"PREPARED BATCH VALIDATED: {batch_id}; files={len(files)}, records={count}")
        return {"batch_id": batch_id, "manifest": str(manifest_path),
                "files": len(files), "records": count}

    @task(retries=0, execution_timeout=timedelta(minutes=20))
    def upload_gcs(batch: dict) -> dict:
        call_cli(
            [sys.executable, str(UPLOADER), "--upload", "--manifest", batch["manifest"],
             "--checkpoints-json", str(CHECKPOINT)], timeout=1000,
        )
        check = call_cli(
            [sys.executable, str(UPLOADER), "--check", "--manifest", batch["manifest"],
             "--checkpoints-json", str(CHECKPOINT)], timeout=180,
        )
        if "GCS CHECK PASSED" not in check or "missing objects=0" not in check:
            raise RuntimeError("GCS did not confirm all objects were uploaded and verified")
        print("GCS UPLOAD + CHECK PASSED")
        return batch

    @task(retries=0, execution_timeout=timedelta(minutes=30))
    def load_bigquery(batch: dict) -> dict:
        output = call_cli(
            [sys.executable, str(LOADER), "--apply", "--manifest", batch["manifest"],
             "--checkpoints-json", str(CHECKPOINT)], timeout=1500,
        )
        if "BIGQUERY BATCH APPLY PASS" not in output:
            raise RuntimeError("BigQuery batch loader did not report success")
        check = call_cli(
            [sys.executable, str(LOADER), "--check", "--manifest", batch["manifest"],
             "--checkpoints-json", str(CHECKPOINT)], timeout=600,
        )
        expected = f"ready=0; skipped={batch['files']}"
        if "BIGQUERY BATCH CHECK PASSED" not in check or expected not in check:
            raise RuntimeError(f"BigQuery replay verification failed; expected {expected}")
        print("BIGQUERY LOADING + REPLAY CHECK PASSED")
        return batch

    @task(retries=0, execution_timeout=timedelta(minutes=50))
    def build_and_test_dbt(batch: dict) -> dict:
        with tempfile.TemporaryDirectory(prefix="day8_batch_dbt_") as work_dir:
            target = Path(work_dir) / "target"
            logs = Path(work_dir) / "logs"
            target.mkdir()
            logs.mkdir()
            environment = os.environ.copy()
            environment["DBT_ENGINE_TARGET_PATH"] = str(target)
            environment["DBT_ENGINE_LOG_PATH"] = str(logs)
            call_cli(
                [str(DBT), "build", "--project-dir", str(DBT_PROJECT),
                 "--profiles-dir", str(DBT_PROFILES)],
                timeout=2400, env=environment,
            )
            report = target / "run_results.json"
            if not report.is_file():
                raise RuntimeError("dbt run_results.json missing; refuse QA confirmation")
            results = json.loads(report.read_text(encoding="utf-8"))["results"]
            models = [r for r in results if r.get("unique_id", "").startswith("model.")]
            tests = [r for r in results if r.get("unique_id", "").startswith("test.")]
            errors = [(r.get("unique_id"), r.get("status")) for r in results
                      if r.get("status") not in ("success", "pass")]
            if len(models) != 11 or len(tests) != 76 or len(results) != 87 or errors:
                raise RuntimeError(
                    f"dbt QA not fully successful: models={len(models)}, "
                    f"tests={len(tests)}, total={len(results)}, errors={errors[:8]}"
                )
            print("DBT BUILD QA PASS: 11 models + 76 tests = 87/87")
            batch["qa_results"] = 87
            return batch

    @task(retries=0, execution_timeout=timedelta(minutes=15))
    def verify_finalization_ready(batch: dict) -> dict:
        # The finalizer verifies BigQuery RAW/registry and GCS consistency.
        # Only its --check mode is allowed in this DAG. No --qa-confirmed here.
        output = call_cli(
            [sys.executable, str(FINALIZER), "--check", "--manifest", batch["manifest"],
             "--checkpoints-json", str(CHECKPOINT)], timeout=600,
        )
        expected = f"BATCH FINALIZE CHECK PASS: {batch['batch_id']}; state=READY"
        if expected not in output:
            raise RuntimeError("Finalizer did not report state READY; do not commit")
        return batch

    @task(retries=0)
    def finish(batch: dict) -> None:
        print(f"DAY 8.7.10.10 MANUAL PIPELINE QA PASSED: {batch['batch_id']}")
        print(f"Verified {batch['files']} file(s) / {batch['records']} row(s); dbt 87/87")
        print("NO WATERMARK COMMIT performed by this DAG")
        print("NEXT: review downstream SQL, then finalize manually on Windows")
        print("Do NOT prepare another batch before committed checkpoint is finalized/exported")

    finish(verify_finalization_ready(build_and_test_dbt(load_bigquery(upload_gcs(validate_pending_batch())))))


day8_batch_pipeline_manual()

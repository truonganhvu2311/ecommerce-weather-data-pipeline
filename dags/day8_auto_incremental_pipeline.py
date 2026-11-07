"""Day 8.7.10.13: GUARDED Airflow 3 demo, PostgreSQL -> GCS -> BQ -> dbt -> checkpoint.

Preconditions:
- All Day 8.7.10.12 worker preflight tasks PASS.
- Source /opt/airflow/de_project/src is mounted read-only.
- /opt/airflow/de_project/data/airflow_batches and .../airflow_batches_auto
  are TWO WRITABLE MOUNTS of the SAME host directory.
- Airflow Connection postgres_ecommerce_source works.
- Google ADC and /opt/dbt-venv/bin/dbt work; 11 models / 76 tests.

Safety:
- schedule=None, max_active_runs=1, retries=0; only manual triggering.
- Explicit DAG Run conf confirm_write=RUN_AUTO_PIPELINE required.
- Stable auto_<run_id sha256> per DAG Run; never re-extract on retry.
- BigQuery de_etl.watermarks is authoritative (no legacy local checkpoint).
- No BigQuery writes for an empty batch; no checkpoint advance for empty batch.
- Prevent starting a new run if an older local auto batch is uncommitted.
- No automatic task retries for mutation; recover/inspect before clearing tasks.
- The Windows verified checkpoint JSON from previous manual runs is NOT updated.
- Not production-grade CDC: timestamp extraction can miss late commits/hard deletes.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pendulum
from airflow.sdk import dag, task, get_current_context

PROJECT = "weather-data-project-512207"
REGION = "asia-southeast1"
TABLES = ("customers", "products", "orders", "order_items")
PG_CONN_ID = "postgres_ecommerce_source"
BASE = Path("/opt/airflow/de_project")
SRC = BASE / "src"
EXTRACTOR = SRC / "ingestion" / "extract_incremental_airflow.py"
VALIDATOR = SRC / "ingestion" / "validate_batch_manifest.py"
UPLOADER = SRC / "loading" / "upload_airflow_batch_gcs.py"
LOADER = SRC / "loading" / "load_airflow_batch_bigquery.py"
FINALIZER = SRC / "loading" / "finalize_airflow_batch.py"
EXTRACTOR_OUTPUT = BASE / "data" / "airflow_batches"
BATCH_ROOT = BASE / "airflow_batches_auto"  # SAME HOST FOLDER as EXTRACTOR_OUTPUT
STATE_DIR = BATCH_ROOT / ".checkpoints"
DBT = Path("/opt/dbt-venv/bin/dbt")
DBT_PROJECT = BASE / "dbt"
DBT_PROFILES = BASE / "profiles"


def call_cli(argv: list[str], timeout_seconds: int, *, env: dict | None = None) -> str:
    """No shell or user-supplied executable; do not log PG credentials."""
    proc = subprocess.run(
        argv, capture_output=True, text=True, encoding="utf-8", errors="replace",
        check=False, timeout=timeout_seconds, env=env,
    )
    output = (proc.stdout or "") + "\n" + (proc.stderr or "")
    for line in output.splitlines()[-90:]:
        print(line)
    if proc.returncode != 0:
        raise RuntimeError(
            f"Failed process (exit {proc.returncode}); inspect logs. "
            "DO NOT create a new DAG Run or delete batch files before reconciliation."
        )
    return output


def timestamp_utc(value) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Invalid timezone-aware watermark")
    return value.astimezone(timezone.utc)


def batch_name(run_id: str) -> str:
    # Stable across task retries; no secrets or personal information in the ID.
    return "auto_" + hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:24]


def live_checkpoints(client):
    from google.cloud import bigquery
    sql = f"""SELECT source_table, committed_watermark, committed_batch_id
              FROM `{PROJECT}.de_etl.watermarks`"""
    rows = list(client.query(sql, location=REGION).result())
    if len(rows) != 4 or {r.source_table for r in rows} != set(TABLES):
        raise RuntimeError("Live BigQuery watermarks must contain exactly four unique tables")
    if any(r.committed_watermark is None or not r.committed_batch_id for r in rows):
        raise RuntimeError("Missing committed watermark / batch id in live BigQuery")
    batch_ids = {r.committed_batch_id for r in rows}
    if len(batch_ids) != 1:
        raise RuntimeError("Live BigQuery checkpoints have inconsistent committed_batch_id")
    last = next(iter(batch_ids))
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("batch", "STRING", last)
    ])
    statuses = list(client.query(
        f"SELECT status FROM `{PROJECT}.de_etl.pipeline_batches` WHERE batch_id=@batch",
        job_config=config, location=REGION).result())
    if len(statuses) != 1 or statuses[0].status != "committed":
        raise RuntimeError("Latest live checkpoint batch does not have one committed metadata row")
    return {r.source_table: timestamp_utc(r.committed_watermark) for r in rows}, last


def remote_batch_status(client, candidate: str) -> str | None:
    from google.cloud import bigquery
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("batch", "STRING", candidate)
    ])
    rows = list(client.query(
        f"SELECT status FROM `{PROJECT}.de_etl.pipeline_batches` WHERE batch_id=@batch",
        job_config=config, location=REGION).result())
    if len(rows) > 1:
        raise RuntimeError(f"Duplicate pipeline_batches metadata for {candidate}")
    return rows[0].status if rows else None


def guard_incomplete_batches(client, current_batch: str) -> None:
    """Prevent a fresh run from skipping a known unfinished local auto batch."""
    if not BATCH_ROOT.is_dir() or not EXTRACTOR_OUTPUT.is_dir():
        raise FileNotFoundError("Expected Day 8.7.10.12 two writable batch mounts")
    for d in BATCH_ROOT.iterdir():
        if not d.is_dir() or d.name == current_batch:
            continue
        if d.name.startswith(".auto_") and ".tmp-" in d.name:
            raise RuntimeError(f"Unfinished extract temp folder {d.name}; audit it first")
        if not d.name.startswith("auto_"):
            continue
        manifest_path = d / "manifest.json"
        if not manifest_path.is_file():
            raise RuntimeError(f"Auto batch directory without manifest: {d}")
        doc = json.loads(manifest_path.read_text(encoding="utf-8"))
        if doc.get("batch_id") != d.name:
            raise RuntimeError(f"Mismatched batch name/manifest: {d}")
        if not doc.get("files"):
            continue  # Old EMPTY batches are intentionally not committed.
        state = remote_batch_status(client, d.name)
        if state != "committed":
            raise RuntimeError(
                f"Uncommitted older automatic batch {d.name} (BQ status={state}). "
                "Recover the original DAG Run before creating another batch."
            )


def read_batch(bundle: dict) -> dict:
    manifest = Path(bundle["manifest"])
    doc = json.loads(manifest.read_text(encoding="utf-8"))
    if doc.get("batch_id") != bundle["batch_id"] or doc.get("status") != "pending":
        raise RuntimeError("Batch manifest is missing/changed")
    return doc


@dag(
    dag_id="day8_auto_incremental_pipeline",
    description="Guarded automatic EXTRACT -> GCS -> BQ -> dbt QA -> finalize; manual trigger only",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={"confirm_write": "", "cutoff_lag_minutes": 10},
    tags=["day8", "incremental", "auto", "writes", "manual-trigger"],
)
def day8_auto_incremental_pipeline():
    @task(retries=0, execution_timeout=timedelta(minutes=15))
    def prepare_batch() -> dict:
        ctx = get_current_context()
        conf = dict(ctx.get("params", {}))
        conf.update(dict(ctx["dag_run"].conf or {}))
        if conf.get("confirm_write") != "RUN_AUTO_PIPELINE":
            raise ValueError("Manual consent required: confirm_write=RUN_AUTO_PIPELINE")
        lag = conf.get("cutoff_lag_minutes", 10)
        if type(lag) is not int or not 0 <= lag <= 1440:
            raise ValueError("cutoff_lag_minutes must be integer between 0 and 1440")

        batch_id = batch_name(ctx["ti"].run_id)
        expected = (EXTRACTOR, VALIDATOR, UPLOADER, LOADER, FINALIZER,
                    DBT, DBT_PROJECT / "dbt_project.yml", DBT_PROFILES / "profiles.yml",
                    Path(os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/missing/adc")))
        missing = [str(p) for p in expected if not p.is_file()]
        if missing:
            raise FileNotFoundError(f"Worker runtime scripts/ADC missing: {missing}")
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT, location=REGION)
        live, previous_batch = live_checkpoints(client)
        guard_incomplete_batches(client, batch_id)
        current_status = remote_batch_status(client, batch_id)
        if current_status is not None:
            raise RuntimeError(
                f"Current DAG Run already has BQ batch metadata ({current_status}); "
                "do not restart entire committed/partially finalized DAG."
            )

        STATE_DIR.mkdir(parents=True, exist_ok=True)
        source_state = STATE_DIR / f"{batch_id}_before.json"
        desired = {t: live[t].isoformat() for t in TABLES}
        if source_state.exists():
            existing = json.loads(source_state.read_text(encoding="utf-8"))
            if (existing.get("source") != "de_etl.watermarks"
                    or existing.get("verified") is not True
                    or existing.get("committed_watermarks") != desired):
                raise RuntimeError("Persisted run checkpoint differs from live BQ. Stop and recover.")
        else:
            checkpoint = {
                "source": "de_etl.watermarks", "verified": True,
                "project_id": PROJECT, "committed_batch_id": previous_batch,
                "exported_at_utc": datetime.now(timezone.utc).isoformat(),
                "committed_watermarks": desired,
            }
            with source_state.open("x", encoding="utf-8") as f:
                json.dump(checkpoint, f, indent=2, ensure_ascii=False)
                f.write("\n")
        # Do not pass credentials through XCom or expose in output logs.
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        pg_conn = hook.get_conn()
        try:
            with pg_conn.cursor() as cursor:
                cursor.execute("SELECT current_database()")
                if cursor.fetchone()[0] != "ecommerce_source":
                    raise RuntimeError("PostgreSQL Airflow connection points to wrong database")
            pg_conn.rollback()  # PostgreSQL precheck is SELECT-only.
        finally:
            pg_conn.close()
        c = hook.get_connection(PG_CONN_ID)
        if not c.password or not c.host or not c.login:
            raise RuntimeError("Airflow Postgres connection must have host/login/password")
        if c.schema and c.schema != "ecommerce_source":
            raise RuntimeError("Airflow Postgres connection schema is not ecommerce_source")
        pg_env = os.environ.copy()
        pg_env.update({
            "PGHOST": str(c.host), "PGPORT": str(c.port or 5432),
            "PGDATABASE": "ecommerce_source", "PGUSER": str(c.login),
            "PGPASSWORD": str(c.password),
        })
        call_cli([
            sys.executable, str(EXTRACTOR), "--prepare", "--batch-id", batch_id,
            "--checkpoints-json", str(source_state),
            "--cutoff-lag-minutes", str(lag),
        ], timeout_seconds=600, env=pg_env)
        # The alias mount exposes the extract output created at fixed extractor path.
        manifest = BATCH_ROOT / batch_id / "manifest.json"
        if not manifest.is_file():
            raise RuntimeError("Extract output not visible via second writable mount")
        doc = json.loads(manifest.read_text(encoding="utf-8"))
        if doc.get("batch_id") != batch_id:
            raise RuntimeError("Extractor manifest has unexpected batch_id")
        if doc.get("watermark_from") != desired:
            raise RuntimeError("Manifest watermark_from does not match live committed checkpoints")
        check = call_cli([
            sys.executable, str(VALIDATOR), "--manifest", str(manifest),
            "--checkpoints-json", str(source_state),
        ], timeout_seconds=120)
        if "BATCH MANIFEST PREFLIGHT PASS" not in check:
            raise RuntimeError("Batch manifest validation missing success marker")
        files = doc["files"]
        count = sum(item["record_count"] for item in files)
        result = {"batch_id": batch_id, "manifest": str(manifest),
                  "checkpoint": str(source_state), "files": len(files), "records": count}
        print(f"AUTO BATCH PREPARED: {batch_id}; files={len(files)}, records={count}")
        return result

    @task.branch(retries=0)
    def route_batch(batch: dict) -> str:
        if batch["files"] == 0 and batch["records"] == 0:
            print("EMPTY_BATCH: GCS, BigQuery, dbt and watermark commit will be SKIPPED")
            return "finish_empty_batch"
        if batch["files"] <= 0 or batch["records"] <= 0:
            raise RuntimeError("Inconsistent count of files/records")
        return "upload_gcs"

    @task(retries=0)
    def finish_empty_batch(batch: dict):
        print(f"AUTO EMPTY BATCH PASS: {batch['batch_id']}")
        print("NO GCS/BQ/dbt writes and NO watermark commit. Future run rechecks source.")

    @task(retries=0, execution_timeout=timedelta(minutes=20))
    def upload_gcs(batch: dict) -> dict:
        cli = ["--manifest", batch["manifest"], "--checkpoints-json", batch["checkpoint"]]
        call_cli([sys.executable, str(UPLOADER), "--upload", *cli], 1000)
        out = call_cli([sys.executable, str(UPLOADER), "--check", *cli], 180)
        if "GCS CHECK PASSED" not in out or "missing objects=0" not in out:
            raise RuntimeError("GCS object reconciliation incomplete")
        return batch

    @task(retries=0, execution_timeout=timedelta(minutes=30))
    def load_bigquery(batch: dict) -> dict:
        cli = ["--manifest", batch["manifest"], "--checkpoints-json", batch["checkpoint"]]
        out = call_cli([sys.executable, str(LOADER), "--apply", *cli], 1500)
        if "BIGQUERY BATCH APPLY PASS" not in out:
            raise RuntimeError("BigQuery batch loader did not report PASS")
        verify = call_cli([sys.executable, str(LOADER), "--check", *cli], 600)
        if ("BIGQUERY BATCH CHECK PASSED" not in verify
                or f"ready=0; skipped={batch['files']}" not in verify):
            raise RuntimeError("RAW and registry replay audit incomplete")
        return batch

    @task(retries=0, execution_timeout=timedelta(minutes=50))
    def build_and_test_dbt(batch: dict) -> dict:
        with tempfile.TemporaryDirectory(prefix="day8_auto_dbt_") as temp:
            target = Path(temp) / "target"
            logs = Path(temp) / "logs"
            target.mkdir()
            logs.mkdir()
            env = os.environ.copy()
            env.update({"DBT_ENGINE_TARGET_PATH": str(target),
                        "DBT_ENGINE_LOG_PATH": str(logs)})
            call_cli([
                str(DBT), "build", "--project-dir", str(DBT_PROJECT),
                "--profiles-dir", str(DBT_PROFILES),
            ], 2400, env=env)
            results_path = target / "run_results.json"
            if not results_path.is_file():
                raise RuntimeError("dbt run_results.json not produced")
            results = json.loads(results_path.read_text(encoding="utf-8"))["results"]
            models = [x for x in results if x.get("unique_id", "").startswith("model.")]
            tests = [x for x in results if x.get("unique_id", "").startswith("test.")]
            bad = [(x.get("unique_id"), x.get("status")) for x in results
                   if x.get("status") not in ("success", "pass")]
            if len(models) != 11 or len(tests) != 76 or len(results) != 87 or bad:
                raise RuntimeError(
                    f"dbt QA incomplete: {len(models)} models, {len(tests)} tests, "
                    f"{len(results)} total, nonpass={bad[:8]}"
                )
            print("AUTO DBT QA PASS: 11 models + 76 tests = 87/87")
        return batch

    @task(retries=0, execution_timeout=timedelta(minutes=15))
    def verify_finalization_ready(batch: dict) -> dict:
        out = call_cli([
            sys.executable, str(FINALIZER), "--check", "--manifest", batch["manifest"],
            "--checkpoints-json", batch["checkpoint"],
        ], 600)
        expected = f"BATCH FINALIZE CHECK PASS: {batch['batch_id']}; state=READY"
        if expected not in out:
            raise RuntimeError("Finalizer must be READY before committing metadata")
        return batch

    @task(retries=0, execution_timeout=timedelta(minutes=15))
    def commit_watermarks(batch: dict) -> dict:
        # --qa-confirmed is allowed ONLY after build_and_test_dbt + ready check succeeded.
        out = call_cli([
            sys.executable, str(FINALIZER), "--apply", "--qa-confirmed",
            "--manifest", batch["manifest"],
            "--checkpoints-json", batch["checkpoint"],
        ], 600)
        if "BATCH FINALIZATION APPLY PASS" not in out:
            raise RuntimeError("No confirmed transaction completion; inspect BigQuery manually")
        print("BIGQUERY WATERMARK COMMITTED. Source of truth is de_etl.watermarks.")
        return batch

    @task(retries=0, execution_timeout=timedelta(minutes=15))
    def export_checkpoint(batch: dict) -> dict:
        exported = STATE_DIR / f"{batch['batch_id']}_after.json"
        if not exported.is_file():
            out = call_cli([
                sys.executable, str(FINALIZER), "--export", "--manifest", batch["manifest"],
                "--output", str(exported),
            ], 600)
            if "CHECKPOINT EXPORT PASS" not in out:
                raise RuntimeError("Post-commit checkpoint export not verified")
        doc = json.loads(exported.read_text(encoding="utf-8"))
        if (doc.get("source") != "de_etl.watermarks" or doc.get("verified") is not True
                or doc.get("committed_batch_id") != batch["batch_id"]
                or set(doc.get("committed_watermarks", {})) != set(TABLES)):
            raise RuntimeError("Exported checkpoint is incomplete or does not match batch")
        out = call_cli([
            sys.executable, str(FINALIZER), "--check", "--manifest", batch["manifest"],
            "--checkpoints-json", str(exported),
        ], 600)
        if f"BATCH FINALIZE CHECK PASS: {batch['batch_id']}; state=COMMITTED" not in out:
            raise RuntimeError("Exported checkpoint and BQ metadata not reconciled")
        print(f"AUTO CHECKPOINT EXPORTED AND VERIFIED: {exported}")
        print("Previous Windows checkpoint snapshot is unchanged and may be STALE.")
        return batch

    @task(retries=0)
    def finish(batch: dict):
        print(f"DAY 8.7.10.13 AUTO PIPELINE PASS: {batch['batch_id']}")
        print(f"files={batch['files']}, rows={batch['records']}; dbt 87/87")
        print("GCS + BigQuery RAW + registry verified; 4 watermarks COMMITTED")
        print("DAG remains UNSCHEDULED. Recovery/replay tests are still required.")

    prepared = prepare_batch()
    route = route_batch(prepared)
    empty = finish_empty_batch(prepared)
    uploaded = upload_gcs(prepared)
    route >> [empty, uploaded]
    finish(export_checkpoint(commit_watermarks(verify_finalization_ready(
        build_and_test_dbt(load_bigquery(uploaded))))))


day8_auto_incremental_pipeline()

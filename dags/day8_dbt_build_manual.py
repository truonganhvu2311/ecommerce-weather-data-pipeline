"""Day 8.7.9: MANUAL dbt BigQuery build via Apache Airflow 3.

IMPORTANT: This DAG executes `dbt build`. It creates/replaces dbt models in
BigQuery and runs tests. It does NOT load RAW data, extract PostgreSQL, or
modify de_etl.watermarks. Never enable a recurring schedule for this DAG.
"""

import json
import os
import subprocess
import tempfile
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

DBT = "/opt/dbt-venv/bin/dbt"
PROJECT = "/opt/airflow/de_project/dbt"
PROFILES = "/opt/airflow/de_project/profiles"
EXPECTED_MODELS = 11
EXPECTED_TESTS = 76


@dag(
    dag_id="day8_dbt_build_manual",
    description="Manually build and test existing BigQuery dbt views",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["day8", "dbt", "manual", "bigquery-writes"],
)
def day8_dbt_build_manual():
    @task(retries=0)
    def verify_runtime():
        required = [
            DBT,
            f"{PROJECT}/dbt_project.yml",
            f"{PROFILES}/profiles.yml",
            os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        ]
        missing = [path for path in required if not path or not Path(path).is_file()]
        if missing:
            raise FileNotFoundError(f"Required dbt runtime files missing: {missing}")
        print("DBT RUNTIME PASS: executable, project, profile, and ADC file are present")
        return True

    @task(retries=0, execution_timeout=timedelta(minutes=45))
    def run_dbt_build(runtime_ok):
        if not runtime_ok:
            raise RuntimeError("dbt preflight failed")

        # Target and log directories must be writable. The dbt project is read-only.
        with tempfile.TemporaryDirectory(prefix="day8_dbt_build_") as root:
            root = Path(root)
            target = root / "target"
            logs = root / "logs"
            target.mkdir()
            logs.mkdir()

            env = os.environ.copy()
            env["DBT_ENGINE_TARGET_PATH"] = str(target)
            env["DBT_ENGINE_LOG_PATH"] = str(logs)

            command = [
                DBT, "build",
                "--project-dir", PROJECT,
                "--profiles-dir", PROFILES,
            ]
            print("Starting MANUAL dbt build: existing BigQuery models + tests")
            print("WARNING: This task creates/replaces dbt model views in BigQuery.")
            print("It does not load RAW data or modify committed watermarks.")
            completed = subprocess.run(
                command,
                env=env,
                text=True,
                capture_output=True,
                timeout=2400,
                check=False,
            )
            lines = (completed.stdout + "\n" + completed.stderr).splitlines()
            print("\n".join(lines[-110:]))
            if completed.returncode != 0:
                raise RuntimeError(f"dbt build failed (exit code {completed.returncode})")

            results_path = target / "run_results.json"
            if not results_path.is_file():
                raise FileNotFoundError("Missing dbt run_results.json; build cannot be verified")
            results = json.loads(results_path.read_text(encoding="utf-8")).get("results", [])
            model_results = [r for r in results if r.get("unique_id", "").startswith("model.")]
            test_results = [r for r in results if r.get("unique_id", "").startswith("test.")]
            bad = [
                (r.get("unique_id"), r.get("status"))
                for r in results if r.get("status") not in ("success", "pass")
            ]
            summary = {
                "models": len(model_results),
                "tests": len(test_results),
                "total": len(results),
                "non_pass": len(bad),
            }
            print(f"DBT RUN RESULTS: {summary}")
            if bad:
                raise RuntimeError(f"dbt results have WARN/ERROR/SKIP: {bad[:10]}")
            if summary != {"models": EXPECTED_MODELS, "tests": EXPECTED_TESTS,
                           "total": EXPECTED_MODELS + EXPECTED_TESTS, "non_pass": 0}:
                raise RuntimeError(f"Unexpected dbt build resources/statuses: {summary}")
            return summary

    @task(retries=0)
    def finish(summary):
        if summary["total"] != EXPECTED_MODELS + EXPECTED_TESTS:
            raise RuntimeError("dbt build count mismatch")
        print("DAY 8.7.9 DBT BUILD PASSED: 11 models + 76 tests = 87 successful results")
        print("This run did NOT upload RAW files or commit ETL watermarks")

    finish(run_dbt_build(verify_runtime()))


day8_dbt_build_manual()

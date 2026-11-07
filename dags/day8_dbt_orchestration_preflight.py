"""Day 8.7.8: read-only Airflow-to-dbt integration smoke test.

Checks dbt executable, BigQuery connection, and exact expected model names.
This DAG deliberately does not run dbt build or write warehouse objects.
"""

import json
import os
import subprocess
import tempfile
from pathlib import Path

# pyrefly: ignore[missing-import]
import pendulum
# pyrefly: ignore[missing-import]
from airflow.sdk import dag, task

DBT = "/opt/dbt-venv/bin/dbt"
PROJECT = "/opt/airflow/de_project/dbt"
PROFILES = "/opt/airflow/de_project/profiles"
EXPECTED_MODELS = {
    "stg_customers",
    "stg_products",
    "stg_orders",
    "stg_order_items",
    "stg_weather_daily",
    "int_orders_eligible",
    "int_order_items_by_order",
    "int_orders_weather",
    "fct_orders",
    "fct_order_items",
    "mart_city_day",
}


def run_dbt(*arguments):
    with tempfile.TemporaryDirectory(prefix="airflow_dbt_preflight_") as workdir:
        environment = os.environ.copy()
        environment["DBT_ENGINE_TARGET_PATH"] = f"{workdir}/target"
        environment["DBT_ENGINE_LOG_PATH"] = f"{workdir}/logs"
        cmd = [DBT, *arguments, "--project-dir", PROJECT, "--profiles-dir", PROFILES]
        result = subprocess.run(
            cmd,
            env=environment,
            capture_output=True,
            text=True,
            timeout=600,
            check=False,
        )
        if result.returncode != 0:
            tail = "\n".join((result.stdout + "\n" + result.stderr).splitlines()[-35:])
            raise RuntimeError(f"dbt exited with {result.returncode}:\n{tail}")
        return result.stdout


@dag(
    dag_id="day8_dbt_orchestration_preflight",
    description="Read-only dbt Core + BigQuery Airflow integration",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["day8", "dbt", "read-only"],
)
def day8_dbt_orchestration_preflight():
    @task
    def verify_runtime():
        for path in (
            DBT,
            f"{PROJECT}/dbt_project.yml",
            f"{PROFILES}/profiles.yml",
            os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        ):
            if not path or not Path(path).is_file():
                raise FileNotFoundError(f"Missing required runtime file: {path}")
        version = subprocess.run(
            [DBT, "--version"], capture_output=True, text=True,
            timeout=60, check=False,
        )
        if version.returncode:
            raise RuntimeError(version.stderr or version.stdout)
        print("DBT RUNTIME FILES PASS")
        print(version.stdout)
        return True

    @task
    def verify_bigquery_connection(runtime_ok):
        if not runtime_ok:
            raise RuntimeError("Runtime preflight failed")
        output = run_dbt("debug")
        if "All checks passed!" not in output:
            raise RuntimeError("dbt debug did not report all checks passed")
        print("DBT DEBUG PASS: Connection test successful")
        return True

    @task
    def verify_11_models(connection_ok):
        if not connection_ok:
            raise RuntimeError("dbt connection preflight failed")
        output = run_dbt("ls", "--resource-type", "model", "--output", "json", "--log-level", "warn")
        found = set()
        for line in output.splitlines():
            if not line.lstrip().startswith("{"):
                continue
            try:
                node = json.loads(line)
            except json.JSONDecodeError:
                continue
            if node.get("resource_type") == "model":
                found.add(node["name"])
        if found != EXPECTED_MODELS:
            missing = sorted(EXPECTED_MODELS - found)
            extra = sorted(found - EXPECTED_MODELS)
            raise RuntimeError(f"Models differ: missing={missing}, extra={extra}")
        print(f"DBT MODEL LIST PASS: {len(found)} correct models")
        return len(found)

    @task
    def finish(model_count):
        assert model_count == 11
        print("DAY 8.7.8 DBT ORCHESTRATION PREFLIGHT PASSED")
        print("No dbt build, no RAW load, no committed watermark changes")

    finish(verify_11_models(verify_bigquery_connection(verify_runtime())))


day8_dbt_orchestration_preflight()

"""Day 8.7: safe, read-only integration preflight for Airflow 3.

The tasks only check mounted source paths and run the extractor's self-check.
No PostgreSQL, GCS or BigQuery connection is made and no checkpoint is written.
"""

from pathlib import Path
import subprocess
import sys

import pendulum
from airflow.sdk import dag, task

PROJECT = Path('/opt/airflow/de_project')
EXTRACTOR = PROJECT / 'src' / 'ingestion' / 'extract_incremental_airflow.py'
LOADER = PROJECT / 'src' / 'loading' / 'load_raw_to_bigquery.py'
DBT_CONFIG = PROJECT / 'dbt' / 'dbt_project.yml'


@dag(
    dag_id='day8_integration_preflight',
    description='Verify read-only project mounts and Python extractor self-check',
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz='UTC'),
    catchup=False,
    max_active_runs=1,
    tags=['day8', 'integration', 'read-only'],
)
def day8_integration_preflight():
    @task
    def verify_mounts() -> bool:
        expected_files = (EXTRACTOR, LOADER, DBT_CONFIG)
        for path in expected_files:
            if not path.is_file():
                raise FileNotFoundError(f'Expected mounted project file missing: {path}')
            print(f'[OK] Found: {path}')
        return True

    @task
    def run_extractor_self_check(mounts_ok: bool) -> bool:
        if not mounts_ok:
            raise RuntimeError('Project mounts are not ready')
        proc = subprocess.run(
            [sys.executable, str(EXTRACTOR), '--self-check'],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        print(proc.stdout)
        if proc.returncode != 0:
            raise RuntimeError(
                f'Extractor self-check exited with {proc.returncode}: {proc.stderr}'
            )
        if 'SELF-CHECK PASSED' not in proc.stdout:
            raise RuntimeError('Extractor self-check did not report PASS')
        return True

    @task
    def finish(checked: bool):
        if not checked:
            raise RuntimeError('Preflight incomplete')
        print('DAY 8.7 INTEGRATION PREFLIGHT PASSED (READ-ONLY)')
        print('No PostgreSQL / GCS / BigQuery writes were performed.')

    finish(run_extractor_self_check(verify_mounts()))


day8_integration_preflight()

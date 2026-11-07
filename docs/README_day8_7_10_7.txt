DAY 8.7.10.7 - BIGQUERY INCREMENTAL BATCH LOADER

Files:
  load_airflow_batch_bigquery.py    -> put into project/src/loading/
  day8_7_10_7_verify_bigquery.sql   -> optional read-only audit in BigQuery Console

Dependencies (already installed in the earlier .venv_gcp):
  google-cloud-bigquery
  google-cloud-storage
  Project's existing src/ingestion/validate_batch_manifest.py

On Windows, from project root:

$batchId = 'day8_customer_c990_20261104'
$manifest = ".\data\airflow_batches\$batchId\manifest.json"
$checkpoint = '.\state\airflow\committed_watermarks_verified.json'

.\.venv_gcp\Scripts\python.exe .\src\loading\load_airflow_batch_bigquery.py --self-test

# READ-ONLY CLOUD PREFLIGHT - requires ADC, BigQuery job permission.
.\.venv_gcp\Scripts\python.exe .\src\loading\load_airflow_batch_bigquery.py --check --manifest $manifest --checkpoints-json $checkpoint

# ONLY AFTER CHECK PASS (and no other BigQuery loader running):
.\.venv_gcp\Scripts\python.exe .\src\loading\load_airflow_batch_bigquery.py --apply --manifest $manifest --checkpoints-json $checkpoint

# Replay audit (read-only):
.\.venv_gcp\Scripts\python.exe .\src\loading\load_airflow_batch_bigquery.py --check --manifest $manifest --checkpoints-json $checkpoint

Expected initial preflight: ready=1, skipped=0
Expected initial apply: loaded_files=1, skipped_files=0
Expected replay preflight: ready=0, skipped=1

Important:
- The existing legacy migration loader remains untouched.
- BigQuery de_etl.watermarks is NEVER modified in this step.
- BigQuery de_etl.pipeline_batches is NEVER modified in this step.
- dbt is not run in this step.
- A stage table is created during --apply and removed after execution.
- If there is a failure after staging, audit before retry; don't delete registry/RAW manually.
- No overlapping/concurrent loader executions.
- --check runs SELECT queries, and may incur BigQuery query costs.

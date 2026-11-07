Day 8.7.10.15 - ISOLATED FAILURE & RETRY DRILL
=================================================

WHAT THIS TEST PROVES
- Airflow 3 attempts an @task(retries=1) again after a deliberately raised exception.
- Task retry resumes based on a deterministic local marker in a persistent writable folder.
- The latest auto batch is committed in de_etl.pipeline_batches and its 4 watermarks agree.
- The batch's RAW row counts match de_etl.loaded_files per source_file.
- BigQuery checkpoint/registry/raw counts are unchanged by the retry.

WHAT THIS TEST DOES NOT PROVE
- True crash recovery after partially uploaded GCS objects or partially loaded BigQuery batch.
- Timestamp-based CDC correctness under late commits, simultaneous pipelines or hard deletes.
- Safety of enabling automatic retries on GCS/BigQuery/write tasks: KEEP retries=0 in your existing DAG.

REQUIREMENTS
- User previously confirmed day8_auto_incremental_pipeline nonempty branch PASS.
- Latest batch from BigQuery watermarks has batch_id starting 'auto_' and is 'committed'.
- Airflow worker can access Google ADC, project weather-data-project-512207, location asia-southeast1.
- Runtime folder /opt/airflow/de_project/airflow_batches_auto is writable as verified in step 8.7.10.12.
- Other pipeline/loader runs must be idle; BigQuery read-only queries still cost a small amount.

STEP 1 - COPY DAG (PowerShell from project root)
  .\.venv_gcp\Scripts\python.exe -m py_compile .\dags\day8_recovery_retry_drill.py
  Copy-Item .\dags\day8_recovery_retry_drill.py "C:\Users\ADMIN\airflow-day8\dags\day8_recovery_retry_drill.py" -Force

STEP 2 - AIRFLOW DAG PROCESSOR CHECK
  cd "C:\Users\ADMIN\airflow-day8"
  docker compose exec airflow-dag-processor airflow dags list-import-errors

STEP 3 - TRIGGER MANUALLY
  http://localhost:8080/dags -> day8_recovery_retry_drill -> Trigger
  No JSON conf required. This DAG DOES NOT ALTER production DAG/source/cloud metadata.

EXPECTED RUN
  snapshot_committed_state         Success
  fail_once_and_retry              initial failure / Up For Retry -> Success on retry
  verify_no_bigquery_mutations     Success
  finish                           Success
  DAG Run                          Success

EXPECTED FINISH LOG
  DAY 8.7.10.15 RECOVERY DRILL PASS: injected failure recovered on one retry
  NO PostgreSQL/GCS/BigQuery/dbt/watermark writes were performed by this DAG

IF IT FAILS
- Open Logs of the first failed task. DO NOT modify or delete committed BigQuery rows or manifest.
- If latest committed batch is not an auto_ batch, stop and review the BigQuery metadata.
- Do not activate schedules or set retries>0 on actual write tasks based on this drill alone.

EVIDENCE SCREENSHOT SUGGESTIONS
  docs/screenshots/day8_recovery_drill_success.png
  docs/screenshots/day8_recovery_retry_log.png
  docs/screenshots/day8_recovery_metadata_unchanged.png

FOLLOW-UP
  Plan a controlled partial-upload/partial-load recovery exercise in a dedicated
  sandbox dataset/bucket before claiming full production-level recovery guarantees.

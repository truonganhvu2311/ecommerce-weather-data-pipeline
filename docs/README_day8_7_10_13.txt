DAY 8.7.10.13 — GUARDED AUTOMATIC INCREMENTAL DAG (AIRFLOW 3.3.2)
====================================================================

CURRENT STATE
- day8_auto_runtime_preflight has PASS 4/4 on your local Docker Worker.
- Second manual batch day8_customer_c990_update_20261107 was finalized.
- No need to redo manual C990 batches, reinstall dbt or re-upload GCS.

FILE TO ADD
  dags/day8_auto_incremental_pipeline.py

1. COPY THE FILE INTO SOURCE PROJECT
   C:\Users\ADMIN\OneDrive\Máy tính\data-engineering-project\dags\day8_auto_incremental_pipeline.py

2. COPY INTO ACTIVE AIRFLOW DAGS (PowerShell)
   cd 'C:\Users\ADMIN\OneDrive\Máy tính\data-engineering-project'
   .\.venv_gcp\Scripts\python.exe -m py_compile .\dags\day8_auto_incremental_pipeline.py
   Copy-Item .\dags\day8_auto_incremental_pipeline.py 'C:\Users\ADMIN\airflow-day8\dags\day8_auto_incremental_pipeline.py' -Force

3. CHECK DAG PARSE
   cd 'C:\Users\ADMIN\airflow-day8'
   docker compose exec airflow-dag-processor airflow dags list-import-errors
   Check Airflow UI http://localhost:8080/dags
   DAG ID: day8_auto_incremental_pipeline

4. SAFETY CHECK BEFORE ANY TRIGGER
   - Other loaders and manual DAG runs must be idle.
   - Verify BigQuery de_etl.watermarks contains the four committed rows and
     de_etl.pipeline_batches has the previously committed C990 UPDATE batch.
   - The previous 4/4 runtime preflight should still be valid.
   - Source changes may be pending; a real DAG Run may WRITE GCS, BigQuery,
     BigQuery dbt models, and committed metadata.
   - Use MANUAL TRIGGER ONLY; keep schedule=None, no automatic timetable.

5. MANUAL TRIGGER CONFIGURATION (Airflow UI -> Trigger -> JSON conf)
   {
     "confirm_write": "RUN_AUTO_PIPELINE",
     "cutoff_lag_minutes": 10
   }

   This means the DAG is ALLOWED to write if there is eligible new data.
   When no new data is within the watermark window, the EMPTY branch completes
   safely, skips GCS/BQ/dbt, and DOES NOT advance watermarks.

6. EXPECTED TASK RESULTS
   Nonempty (writes/commits):
     prepare_batch                  Success
     route_batch                    Success
     finish_empty_batch             Skipped (EXPECTED)
     upload_gcs                     Success
     load_bigquery                  Success
     build_and_test_dbt             Success (11 models + 76 tests = 87/87)
     verify_finalization_ready      Success (READY)
     commit_watermarks              Success (COMMITTED)
     export_checkpoint              Success (COMMITTED verified)
     finish                         Success

   Empty batch:
     prepare_batch, route_batch, finish_empty_batch = Success;
     all write-path tasks = Skipped. This is an EXPECTED success path.

7. SAFE LOG MESSAGES
   Nonempty: DAY 8.7.10.13 AUTO PIPELINE PASS: auto_<id>
             AUTO DBT QA PASS: 11 models + 76 tests = 87/87
             AUTO CHECKPOINT EXPORTED AND VERIFIED: /opt/airflow/.../auto_<id>_after.json
   Empty:    AUTO EMPTY BATCH PASS: auto_<id>
             NO GCS/BQ/dbt writes and NO watermark commit

HOW IT WORKS
- Before extraction, reads four live committed checkpoints from BigQuery.
- Writes a verified, per-DAG-Run checkpoint snapshot under the *writable*
  Windows folder C:\Users\ADMIN\airflow-day8\runtime\airflow_batches_auto\.checkpoints
- Uses the existing Airflow Postgres connection; credentials are injected to
  the Python extractor subprocess, not stored in the DAG or XCom.
- Stable batch ID derived from Airflow Run ID: 'auto_' + SHA256(run_id)[:24].
  Re-running the SAME task reuses immutable batch files.
- Only moves the BQ watermark after GCS, BQ RAW/registry and dbt 87/87 PASS.
- Exports the new committed checkpoint into the writable runtime.
- The existing Windows project file state/airflow/committed_watermarks_verified.json
  is NOT overwritten. It becomes a historical snapshot. From now on the
  automatic DAG reads the authoritative live BigQuery watermarks at each run.

VERY IMPORTANT / FAILURE RECOVERY
- No task retries are enabled. If a task fails, DO NOT trigger a NEW run or
  delete partial GCS/BQ records or old batches. Inspect the failed task log.
- Partial upload/load may have completed; uploader and loader have repeat-check
  mechanisms, but recovery must reconcile actual state before clearing tasks.
- If commit succeeds but export fails: do not re-commit. Inspect BigQuery and
  run the existing finalize_airflow_batch.py --export against the same manifest;
  export task can then be cleared after the cause is corrected.
- Old, unfinished auto batch with data blocks new runs deliberately.
- Keep previously mounted read-only manual batch and checkpoint paths unchanged.
- DO NOT enable a daily schedule yet: source uses timestamp CDC-like extraction,
  which does not fully address late commits, hard deletes, concurrency and
  independent manual loaders.
- The --qa-confirmed CLI flag in this DAG is set only after the same run checks
  dbt run_results.json for 11 model successes + 76 passing tests and finalizer
  READY. Other business reconciliations may be needed before production.

AFTER THE FIRST MANUAL TEST
Capture screenshots:
 docs/screenshots/day8_auto_pipeline_graph.png
 docs/screenshots/day8_auto_pipeline_commit_qa.png  (only for a NONEMPTY success)

NEXT STEP (DAY 8.7.10.14)
- Inject a controlled failure and verify recovery without duplicating RAW.
- Validate empty batch behavior, retry, watermark integrity and no overlaps.
- Only then consider scheduling. This is a working DEMO, not proven production CDC.

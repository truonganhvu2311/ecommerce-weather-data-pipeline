-- Day 8.7.10.8: downstream reconciliation for the C990 incremental batch.
-- BigQuery project: weather-data-project-512207
-- Location: asia-southeast1
-- READ ONLY: SELECT statements only, no checkpoint commit or table mutations.

DECLARE target_batch STRING DEFAULT 'day8_customer_c990_20261104';

WITH raw_counts AS (
  SELECT
    COUNT(*) AS raw_rows,
    COUNTIF(customer_id = 'C990') AS c990_rows,
    COUNT(DISTINCT source_file) AS distinct_raw_files
  FROM `weather-data-project-512207.de_raw.customers`
  WHERE batch_id = target_batch
),
registry_counts AS (
  SELECT
    COUNT(*) AS registry_rows,
    COALESCE(SUM(record_count), 0) AS registry_record_count,
    COUNTIF(source_type = 'customers' AND record_count = 1) AS registry_customer_files
  FROM `weather-data-project-512207.de_etl.loaded_files`
  WHERE batch_id = target_batch
),
matched_counts AS (
  SELECT COUNT(*) AS raw_registry_matched_rows
  FROM `weather-data-project-512207.de_raw.customers` AS r
  INNER JOIN `weather-data-project-512207.de_etl.loaded_files` AS f
    ON r.source_file = f.source_file
  WHERE r.batch_id = target_batch
    AND f.batch_id = target_batch
),
staging_counts AS (
  SELECT COUNT(*) AS c990_staging_rows
  FROM `weather-data-project-512207.de_staging.stg_customers`
  WHERE customer_id = 'C990'
),
watermark_counts AS (
  SELECT
    COUNT(*) AS committed_checkpoint_rows,
    COUNTIF(committed_batch_id = target_batch) AS prematurely_committed_rows
  FROM `weather-data-project-512207.de_etl.watermarks`
)
SELECT
  target_batch AS batch_id,
  raw.raw_rows,
  raw.c990_rows,
  raw.distinct_raw_files,
  reg.registry_rows,
  reg.registry_record_count,
  reg.registry_customer_files,
  matched.raw_registry_matched_rows,
  stg.c990_staging_rows,
  w.committed_checkpoint_rows,
  w.prematurely_committed_rows,
  CASE WHEN raw.raw_rows = 1
       AND raw.c990_rows = 1
       AND raw.distinct_raw_files = 1
       AND reg.registry_rows = 1
       AND reg.registry_record_count = 1
       AND reg.registry_customer_files = 1
       AND matched.raw_registry_matched_rows = 1
       AND stg.c990_staging_rows = 1
       AND w.committed_checkpoint_rows = 4
       AND w.prematurely_committed_rows = 0
       THEN 'PASS' ELSE 'REVIEW_REQUIRED' END AS audit_status
FROM raw_counts AS raw
CROSS JOIN registry_counts AS reg
CROSS JOIN matched_counts AS matched
CROSS JOIN staging_counts AS stg
CROSS JOIN watermark_counts AS w;

-- Optional: inspect the one ingested source record.
SELECT customer_id, customer_name, city, source_file, batch_id, _watermark_from, _watermark_to
FROM `weather-data-project-512207.de_raw.customers`
WHERE batch_id = target_batch
ORDER BY source_line_number;

-- Optional: confirm checkpoint values have NOT been advanced by this step.
SELECT source_table, committed_watermark, committed_batch_id, committed_at
FROM `weather-data-project-512207.de_etl.watermarks`
ORDER BY source_table;

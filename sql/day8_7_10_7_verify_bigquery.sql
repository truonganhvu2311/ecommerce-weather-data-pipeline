-- Day 8.7.10.7 -- Read-only SQL for BigQuery Console (Singapore / asia-southeast1)
-- This only audits batch day8_customer_c990_20261104; it does NOT modify tables.

-- 1. Expected: one RAW event for C990 in this batch.
SELECT customer_id, customer_name, city, batch_id, source_file,
       source_line_number, _watermark_from, _watermark_to, loaded_at
FROM `weather-data-project-512207.de_raw.customers`
WHERE batch_id = 'day8_customer_c990_20261104'
ORDER BY source_line_number;

-- 2. Expected: one file registry row, record_count=1.
SELECT source_file, source_type, sha256, record_count, gcs_uri,
       batch_id, load_job_id, loaded_at
FROM `weather-data-project-512207.de_etl.loaded_files`
WHERE batch_id = 'day8_customer_c990_20261104';

-- 3. Expected: raw_rows=1; registry_rows=1.
SELECT
  (SELECT COUNT(*) FROM `weather-data-project-512207.de_raw.customers`
   WHERE batch_id = 'day8_customer_c990_20261104') AS raw_rows,
  (SELECT COUNT(*) FROM `weather-data-project-512207.de_etl.loaded_files`
   WHERE batch_id = 'day8_customer_c990_20261104') AS registry_rows;

-- 4. Watermarks must still have the PREVIOUS committed batch id.
SELECT source_table, committed_watermark, committed_batch_id
FROM `weather-data-project-512207.de_etl.watermarks`
ORDER BY source_table;

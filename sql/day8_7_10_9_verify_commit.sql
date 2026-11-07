-- DAY 8.7.10.9: verification AFTER a successful manual metadata commit.
-- GoogleSQL; query location: asia-southeast1. READ ONLY.
DECLARE target_batch STRING DEFAULT 'day8_customer_c990_20261104';

SELECT source_table, committed_watermark, committed_batch_id, committed_at
FROM `weather-data-project-512207.de_etl.watermarks`
ORDER BY source_table;

SELECT batch_id, status, manifest_uri,
       TO_JSON_STRING(watermark_from) AS watermark_from,
       TO_JSON_STRING(watermark_to) AS watermark_to,
       created_at, updated_at
FROM `weather-data-project-512207.de_etl.pipeline_batches`
WHERE batch_id = target_batch;

-- Do not expect the local/GCS pending manifest to change:
-- The authoritative lifecycle status is pipeline_batches.status.
SELECT 'raw_customers' AS component, COUNT(*) AS rows_for_batch
FROM `weather-data-project-512207.de_raw.customers`
WHERE batch_id = target_batch
UNION ALL
SELECT 'loaded_file_registry', COUNT(*)
FROM `weather-data-project-512207.de_etl.loaded_files`
WHERE batch_id = target_batch;

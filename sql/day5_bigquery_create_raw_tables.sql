-- E-commerce Sales & Weather Analytics
-- BigQuery RAW + ETL metadata tables (migration from PostgreSQL DEV)
-- Google Cloud project: weather-data-project-512207
-- Dataset locations MUST be asia-southeast1 (Singapore).
--
-- SAFE TO RE-RUN: CREATE TABLE IF NOT EXISTS avoids replacing tables.
-- IMPORTANT: IF NOT EXISTS does not validate the schema of an existing table.
-- This script creates structures only; it does NOT load, drop, or delete data.
-- Numeric and timestamp mappings are based on the existing Python extractor/loader.
-- Confirm mappings against the actual PostgreSQL schema and sample JSON before loading.

-- 1. Raw customers (each JSONL line is a historical extraction event)
CREATE TABLE IF NOT EXISTS `weather-data-project-512207.de_raw.customers` (
  raw_event_id STRING DEFAULT (GENERATE_UUID()),
  customer_id INT64,
  customer_name STRING,
  city STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _source_table STRING,
  _extracted_at TIMESTAMP,
  _watermark_from TIMESTAMP,
  _watermark_to TIMESTAMP,
  source_file STRING,
  source_line_number INT64,
  gcs_uri STRING,
  batch_id STRING,
  loaded_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP())
)
PARTITION BY DATE(_extracted_at)
CLUSTER BY customer_id
OPTIONS(description='Immutable-like raw customer extract events loaded from GCS JSONL.');

-- 2. Raw products
CREATE TABLE IF NOT EXISTS `weather-data-project-512207.de_raw.products` (
  raw_event_id STRING DEFAULT (GENERATE_UUID()),
  product_id INT64,
  product_name STRING,
  category STRING,
  price NUMERIC,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _source_table STRING,
  _extracted_at TIMESTAMP,
  _watermark_from TIMESTAMP,
  _watermark_to TIMESTAMP,
  source_file STRING,
  source_line_number INT64,
  gcs_uri STRING,
  batch_id STRING,
  loaded_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP())
)
PARTITION BY DATE(_extracted_at)
CLUSTER BY product_id
OPTIONS(description='Raw product extract events, retaining source and extraction metadata.');

-- 3. Raw orders: preserve soft deletes and multiple extracted versions per order
CREATE TABLE IF NOT EXISTS `weather-data-project-512207.de_raw.orders` (
  raw_event_id STRING DEFAULT (GENERATE_UUID()),
  order_id INT64,
  customer_id INT64,
  order_date TIMESTAMP,
  status STRING,
  city STRING,
  total_amount NUMERIC,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_deleted BOOL,
  _source_table STRING,
  _extracted_at TIMESTAMP,
  _watermark_from TIMESTAMP,
  _watermark_to TIMESTAMP,
  source_file STRING,
  source_line_number INT64,
  gcs_uri STRING,
  batch_id STRING,
  loaded_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP())
)
PARTITION BY DATE(_extracted_at)
CLUSTER BY order_id
OPTIONS(description='Raw order events, including status changes and is_deleted for latest-state modeling.');

-- 4. Raw order_items
CREATE TABLE IF NOT EXISTS `weather-data-project-512207.de_raw.order_items` (
  raw_event_id STRING DEFAULT (GENERATE_UUID()),
  order_item_id INT64,
  order_id INT64,
  product_id INT64,
  quantity INT64,
  unit_price NUMERIC,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _source_table STRING,
  _extracted_at TIMESTAMP,
  _watermark_from TIMESTAMP,
  _watermark_to TIMESTAMP,
  source_file STRING,
  source_line_number INT64,
  gcs_uri STRING,
  batch_id STRING,
  loaded_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP())
)
PARTITION BY DATE(_extracted_at)
CLUSTER BY order_id, order_item_id
OPTIONS(description='Raw order line events, retaining all extraction versions.');

-- 5. Raw weather: ONE ROW PER FILE (not one row per city/date)
-- Python loader must parse the original JSON document and populate payload.
CREATE TABLE IF NOT EXISTS `weather-data-project-512207.de_raw.weather_api_responses` (
  source_file STRING,
  payload JSON,
  gcs_uri STRING,
  batch_id STRING,
  loaded_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP())
)
PARTITION BY DATE(loaded_at)
CLUSTER BY source_file
OPTIONS(description='One original Open-Meteo JSON document per ingested weather file, containing metadata and response.');

-- 6. ETL file registry: loader must ensure unique source_file application-side.
-- BigQuery does not enforce uniqueness automatically.
CREATE TABLE IF NOT EXISTS `weather-data-project-512207.de_etl.loaded_files` (
  source_file STRING,
  source_type STRING,
  sha256 STRING,
  record_count INT64,
  gcs_uri STRING,
  batch_id STRING,
  load_job_id STRING,
  loaded_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP())
)
OPTIONS(description='File-level ingestion registry for SHA-256 verification, job replay and idempotency.');

-- 7. ETL batch status for future Airflow/replay workflow.
CREATE TABLE IF NOT EXISTS `weather-data-project-512207.de_etl.pipeline_batches` (
  batch_id STRING,
  status STRING,
  manifest_uri STRING,
  watermark_from JSON,
  watermark_to JSON,
  created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP()),
  updated_at TIMESTAMP
)
OPTIONS(description='Batch lifecycle metadata, for pending/uploaded/loaded/validated/committed/failed states.');

-- 8. Committed watermarks ONLY. Do not populate/reset existing local watermark here.
CREATE TABLE IF NOT EXISTS `weather-data-project-512207.de_etl.watermarks` (
  source_table STRING,
  committed_watermark TIMESTAMP,
  committed_batch_id STRING,
  committed_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP())
)
OPTIONS(description='Committed per-table extraction watermarks; modified only after downstream verification.');

-- IMPORTANT: End of DDL. No INSERT/UPDATE/DELETE statement in this script.

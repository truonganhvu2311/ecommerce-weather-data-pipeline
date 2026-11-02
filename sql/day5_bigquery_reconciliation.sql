-- BigQuery RAW reconciliation (run in BigQuery Studio, location asia-southeast1)
-- Snapshot expectation: same 8 transaction + 3 weather files already loaded into PostgreSQL DEV.
SELECT 'customers' AS source_type, COUNT(*) AS raw_rows FROM `weather-data-project-512207.de_raw.customers`
UNION ALL SELECT 'products', COUNT(*) FROM `weather-data-project-512207.de_raw.products`
UNION ALL SELECT 'orders', COUNT(*) FROM `weather-data-project-512207.de_raw.orders`
UNION ALL SELECT 'order_items', COUNT(*) FROM `weather-data-project-512207.de_raw.order_items`
UNION ALL SELECT 'weather', COUNT(*) FROM `weather-data-project-512207.de_raw.weather_api_responses`;

SELECT source_type, COUNT(*) AS files, SUM(record_count) AS records
FROM `weather-data-project-512207.de_etl.loaded_files`
GROUP BY source_type
ORDER BY source_type;

-- These queries should return 0 rows.
SELECT source_file, source_line_number, COUNT(*) AS copies
FROM (
  SELECT source_file, source_line_number FROM `weather-data-project-512207.de_raw.customers`
  UNION ALL SELECT source_file, source_line_number FROM `weather-data-project-512207.de_raw.products`
  UNION ALL SELECT source_file, source_line_number FROM `weather-data-project-512207.de_raw.orders`
  UNION ALL SELECT source_file, source_line_number FROM `weather-data-project-512207.de_raw.order_items`
)
GROUP BY source_file, source_line_number
HAVING COUNT(*) > 1;

SELECT source_file, COUNT(*) AS copies
FROM `weather-data-project-512207.de_etl.loaded_files`
GROUP BY source_file
HAVING COUNT(*) > 1;

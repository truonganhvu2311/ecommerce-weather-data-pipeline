
WITH source_data AS (
    SELECT *
    FROM {{ source('raw', 'customers') }}
)

SELECT
    TRIM(customer_id) AS customer_id,
    TRIM(customer_name) AS customer_name,
    TRIM(city) AS city,
    created_at,
    updated_at,
    _extracted_at,
    loaded_at,
    raw_event_id,
    raw_event_id AS source_raw_event_id,
    source_file,
    source_line_number
FROM source_data

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY
        updated_at DESC NULLS LAST,
        _extracted_at DESC NULLS LAST,
        loaded_at DESC NULLS LAST,
        raw_event_id DESC NULLS LAST
) = 1
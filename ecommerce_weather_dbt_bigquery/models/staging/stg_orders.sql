
WITH source_data AS (
    SELECT *
    FROM {{ source('raw', 'orders') }}
)

SELECT
    TRIM(order_id) AS order_id,
    TRIM(customer_id) AS customer_id,
    order_date,
    LOWER(TRIM(status)) AS status,
    TRIM(city) AS city,
    total_amount,
    created_at,
    updated_at,
    is_deleted,
    _extracted_at,
    loaded_at,
    raw_event_id,
    source_file,
    source_line_number
FROM source_data

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY
        updated_at DESC NULLS LAST,
        _extracted_at DESC NULLS LAST,
        loaded_at DESC NULLS LAST,
        raw_event_id DESC NULLS LAST
) = 1

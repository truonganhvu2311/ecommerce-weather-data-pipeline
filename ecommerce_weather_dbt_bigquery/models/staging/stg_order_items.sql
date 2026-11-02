
WITH source_data AS (
    SELECT *
    FROM {{ source('raw', 'order_items') }}
)

SELECT
    TRIM(order_item_id) AS order_item_id,
    TRIM(order_id) AS order_id,
    TRIM(product_id) AS product_id,
    quantity,
    unit_price,
    quantity * unit_price AS line_amount,
    created_at,
    updated_at,
    _extracted_at,
    loaded_at,
    raw_event_id,
    source_file,
    source_line_number
FROM source_data

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_item_id
    ORDER BY
        updated_at DESC NULLS LAST,
        _extracted_at DESC NULLS LAST,
        loaded_at DESC NULLS LAST,
        raw_event_id DESC NULLS LAST
) = 1

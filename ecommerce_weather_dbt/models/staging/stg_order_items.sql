
{{ config(materialized='view') }}

WITH source_data AS (

    SELECT
        raw_event_id,

        NULLIF(BTRIM(order_item_id), '') AS order_item_id,
        NULLIF(BTRIM(order_id), '') AS order_id,
        NULLIF(BTRIM(product_id), '') AS product_id,

        quantity,
        unit_price,

        created_at,
        updated_at,

        _extracted_at,
        loaded_at,

        source_file,
        source_line_number

    FROM {{ source('raw', 'order_items') }}

),

ranked_order_items AS (

    SELECT
        *,

        ROW_NUMBER() OVER (
            PARTITION BY order_item_id
            ORDER BY
                updated_at DESC NULLS LAST,
                _extracted_at DESC NULLS LAST,
                loaded_at DESC NULLS LAST,
                raw_event_id DESC
        ) AS row_num

    FROM source_data

),

final AS (

    SELECT
        order_item_id,
        order_id,
        product_id,

        quantity,
        unit_price,

        created_at,
        updated_at,

        _extracted_at AS extracted_at,

        raw_event_id AS source_raw_event_id,
        source_file,
        source_line_number

    FROM ranked_order_items

    WHERE row_num = 1

)

SELECT *
FROM final

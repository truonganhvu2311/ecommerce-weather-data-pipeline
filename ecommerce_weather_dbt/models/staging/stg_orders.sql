
{{ config(materialized='view') }}

WITH source_data AS (

    SELECT
        raw_event_id,

        NULLIF(BTRIM(order_id), '') AS order_id,
        NULLIF(BTRIM(customer_id), '') AS customer_id,

        order_date,

        NULLIF(LOWER(BTRIM(status)), '') AS status,
        NULLIF(BTRIM(city), '') AS city,

        total_amount,

        created_at,
        updated_at,

        is_deleted,

        _extracted_at,
        loaded_at,

        source_file,
        source_line_number

    FROM {{ source('raw', 'orders') }}

),

ranked_orders AS (

    SELECT
        *,

        ROW_NUMBER() OVER (
            PARTITION BY order_id
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
        order_id,
        customer_id,

        order_date,
        status,
        city,

        total_amount,

        created_at,
        updated_at,

        is_deleted,

        _extracted_at AS extracted_at,

        raw_event_id AS source_raw_event_id,
        source_file,
        source_line_number

    FROM ranked_orders

    WHERE row_num = 1

)

SELECT *
FROM final

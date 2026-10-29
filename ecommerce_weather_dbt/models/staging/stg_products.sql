
{{ config(materialized='view') }}

WITH source_data AS (

    SELECT
        raw_event_id,

        NULLIF(BTRIM(product_id), '') AS product_id,
        NULLIF(BTRIM(product_name), '') AS product_name,
        NULLIF(BTRIM(category), '') AS category,

        price,
        created_at,
        updated_at,

        _extracted_at,
        loaded_at,

        source_file,
        source_line_number

    FROM {{ source('raw', 'products') }}

),

ranked_products AS (

    SELECT
        *,

        ROW_NUMBER() OVER (
            PARTITION BY product_id
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
        product_id,
        product_name,
        category,
        price,

        created_at,
        updated_at,

        _extracted_at AS extracted_at,

        raw_event_id AS source_raw_event_id,
        source_file,
        source_line_number

    FROM ranked_products

    WHERE row_num = 1

)

SELECT *
FROM final

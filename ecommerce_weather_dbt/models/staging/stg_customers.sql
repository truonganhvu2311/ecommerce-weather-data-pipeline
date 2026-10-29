
{{ config(materialized='view') }}

WITH source_data AS (

    SELECT
        raw_event_id,

        NULLIF(BTRIM(customer_id), '') AS customer_id,
        customer_name,
        city,

        created_at,
        updated_at,
        _extracted_at,
        loaded_at,

        source_file,
        source_line_number

    FROM {{ source('raw', 'customers') }}

),

ranked_customers AS (

    SELECT
        *,

        ROW_NUMBER() OVER (
            PARTITION BY customer_id
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
        customer_id,

        NULLIF(BTRIM(customer_name), '') AS customer_name,
        NULLIF(BTRIM(city), '') AS city,

        created_at,
        updated_at,

        _extracted_at AS extracted_at,

        raw_event_id AS source_raw_event_id,
        source_file,
        source_line_number

    FROM ranked_customers

    WHERE row_num = 1

)

SELECT *
FROM final

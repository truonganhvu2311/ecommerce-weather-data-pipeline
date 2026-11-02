
WITH raw_files AS (
    SELECT
        source_file,
        loaded_at,

        JSON_VALUE(
            payload, '$.metadata.city'
        ) AS city,

        JSON_VALUE(
            payload, '$.metadata.source'
        ) AS source,

        SAFE_CAST(
            JSON_VALUE(
                payload, '$.metadata.extracted_at_utc'
            ) AS TIMESTAMP
        ) AS extracted_at_utc,

        JSON_QUERY_ARRAY(
            payload, '$.response.daily.time'
        ) AS day_values,

        JSON_QUERY_ARRAY(
            payload, '$.response.daily.temperature_2m_mean'
        ) AS temperature_values,

        JSON_QUERY_ARRAY(
            payload, '$.response.daily.rain_sum'
        ) AS rain_values

    FROM {{ source('raw', 'weather_api_responses') }}
),

expanded AS (
    SELECT
        TRIM(r.city) AS city,

        SAFE_CAST(
            JSON_VALUE(day_json) AS DATE
        ) AS weather_date,

        SAFE_CAST(
            JSON_VALUE(
                r.temperature_values[SAFE_OFFSET(day_index)]
            ) AS FLOAT64
        ) AS temperature_mean_c,

        SAFE_CAST(
            JSON_VALUE(
                r.rain_values[SAFE_OFFSET(day_index)]
            ) AS FLOAT64
        ) AS rain_sum_mm,

        r.source,
        r.extracted_at_utc,
        r.source_file,
        r.loaded_at

    FROM raw_files AS r
    CROSS JOIN UNNEST(r.day_values)
        AS day_json WITH OFFSET AS day_index
)

SELECT
    city,
    weather_date,
    temperature_mean_c,
    rain_sum_mm,
    source,
    extracted_at_utc,
    source_file,
    source_file AS source_raw_event_id
FROM expanded

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY LOWER(TRIM(city)), weather_date
    ORDER BY
        extracted_at_utc DESC NULLS LAST,
        loaded_at DESC NULLS LAST,
        source_file DESC
) = 1

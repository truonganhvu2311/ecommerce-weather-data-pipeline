
{{ config(materialized='view') }}

WITH raw_responses AS (

    SELECT
        raw_event_id,

        NULLIF(
            BTRIM(payload #>> '{metadata,city}'),
            ''
        ) AS city,

        payload #>> '{response,timezone}' AS api_timezone,

        payload #> '{response,daily}' AS daily_data,

        source_file,
        loaded_at

    FROM {{ source('raw', 'weather_api_responses') }}

),

expanded_days AS (

    SELECT
        r.raw_event_id,
        r.city,
        r.api_timezone,

        d.day_text::date AS weather_date,
        (d.day_position - 1)::integer AS day_index,

        r.daily_data,
        r.source_file,
        r.loaded_at

    FROM raw_responses r

    CROSS JOIN LATERAL
        jsonb_array_elements_text(
            r.daily_data -> 'time'
        ) WITH ORDINALITY
        AS d(day_text, day_position)

),

normalized AS (

    SELECT
        city,
        weather_date,

        (
            (daily_data -> 'precipitation_sum')
            ->> day_index
        )::numeric AS precipitation_sum_mm,

        (
            (daily_data -> 'rain_sum')
            ->> day_index
        )::numeric AS rain_sum_mm,

        (
            (daily_data -> 'temperature_2m_max')
            ->> day_index
        )::numeric AS temperature_max_c,

        (
            (daily_data -> 'temperature_2m_min')
            ->> day_index
        )::numeric AS temperature_min_c,

        api_timezone,
        raw_event_id,
        source_file,
        loaded_at

    FROM expanded_days

),

ranked_weather AS (

    SELECT
        *,

        ROW_NUMBER() OVER (
            PARTITION BY city, weather_date
            ORDER BY
                loaded_at DESC NULLS LAST,
                raw_event_id DESC
        ) AS row_num

    FROM normalized

)

SELECT
    city,
    weather_date,

    precipitation_sum_mm,
    rain_sum_mm,

    temperature_max_c,
    temperature_min_c,

    api_timezone,

    raw_event_id AS source_raw_event_id,
    source_file,
    loaded_at

FROM ranked_weather

WHERE row_num = 1

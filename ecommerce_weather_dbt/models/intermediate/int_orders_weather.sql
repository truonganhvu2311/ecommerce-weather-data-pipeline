
{{ config(materialized='view') }}

WITH orders AS (

    SELECT
        order_id,
        customer_id,
        order_date,
        order_date_vn,
        status,
        city,
        total_amount

    FROM {{ ref('fct_orders') }}

),

weather AS (

    SELECT
        city,
        weather_date,
        precipitation_sum_mm,
        rain_sum_mm,
        temperature_max_c,
        temperature_min_c,
        api_timezone

    FROM {{ ref('stg_weather_daily') }}

),

final AS (

    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_date_vn,
        o.city,
        o.status,
        o.total_amount,

        w.city AS weather_city,
        w.weather_date,

        w.precipitation_sum_mm,
        w.rain_sum_mm,

        w.temperature_max_c,
        w.temperature_min_c,
        w.api_timezone,

        (w.weather_date IS NOT NULL) AS has_weather,

        CASE
            WHEN w.weather_date IS NULL
                OR w.rain_sum_mm IS NULL
                THEN 'unknown'

            WHEN w.rain_sum_mm > 0
                THEN 'rainy'

            ELSE 'dry'
        END AS weather_condition

    FROM orders o

    LEFT JOIN weather w
        ON LOWER(BTRIM(o.city))
           = LOWER(BTRIM(w.city))

       AND o.order_date_vn = w.weather_date

)

SELECT *
FROM final

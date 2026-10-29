
{{ config(materialized='view') }}

WITH orders_by_city_day AS (

    SELECT
        NULLIF(LOWER(BTRIM(city)), '') AS city_key,
        order_date_vn AS report_date,

        MIN(city) AS order_city,

        COUNT(*) AS order_count,

        SUM(total_amount) AS total_order_amount,

        COUNT(*) FILTER (
            WHERE status = 'paid'
        ) AS paid_orders,

        COUNT(*) FILTER (
            WHERE status = 'shipped'
        ) AS shipped_orders,

        COUNT(*) FILTER (
            WHERE status = 'delivered'
        ) AS delivered_orders

    FROM {{ ref('fct_orders') }}

    GROUP BY
        NULLIF(LOWER(BTRIM(city)), ''),
        order_date_vn

),

weather_by_city_day AS (

    SELECT
        NULLIF(LOWER(BTRIM(city)), '') AS city_key,
        weather_date AS report_date,

        city AS weather_city,

        precipitation_sum_mm,
        rain_sum_mm,
        temperature_max_c,
        temperature_min_c,
        api_timezone

    FROM {{ ref('stg_weather_daily') }}

),

combined AS (

    SELECT
        COALESCE(o.city_key, w.city_key) AS city_key,

        COALESCE(w.weather_city, o.order_city) AS city,

        COALESCE(o.report_date, w.report_date)
            AS report_date,

        COALESCE(o.order_count, 0)
            AS order_count,

        COALESCE(o.total_order_amount, 0)
            AS total_order_amount,

        COALESCE(o.paid_orders, 0)
            AS paid_orders,

        COALESCE(o.shipped_orders, 0)
            AS shipped_orders,

        COALESCE(o.delivered_orders, 0)
            AS delivered_orders,

        CASE
            WHEN o.order_count > 0
                THEN o.total_order_amount / o.order_count
            ELSE NULL
        END AS avg_order_value,

        w.precipitation_sum_mm,
        w.rain_sum_mm,
        w.temperature_max_c,
        w.temperature_min_c,
        w.api_timezone,

        (o.report_date IS NOT NULL) AS has_orders,

        (w.report_date IS NOT NULL) AS has_weather,

        CASE
            WHEN w.report_date IS NULL
                 OR w.rain_sum_mm IS NULL
                THEN 'unknown'

            WHEN w.rain_sum_mm > 0
                THEN 'rainy'

            ELSE 'dry'
        END AS weather_condition

    FROM orders_by_city_day o

    FULL OUTER JOIN weather_by_city_day w
        ON o.city_key = w.city_key
       AND o.report_date = w.report_date

)

SELECT *
FROM combined

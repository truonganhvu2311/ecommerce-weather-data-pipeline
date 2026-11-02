
WITH orders_city_day AS (

    -- Tong hop orders TRUOC khi JOIN weather
    SELECT
        LOWER(TRIM(city)) AS city_key,
        order_date_vn AS report_date,

        COUNT(*) AS order_count,

        SUM(total_amount) AS total_order_amount,

        COUNTIF(status = 'paid')
            AS paid_order_count,

        COUNTIF(status = 'shipped')
            AS shipped_order_count,

        COUNTIF(status = 'delivered')
            AS delivered_order_count

    FROM {{ ref('fct_orders') }}

    GROUP BY 1, 2

),

weather_city_day AS (

    SELECT
        LOWER(TRIM(city)) AS city_key,
        weather_date AS report_date,

        temperature_mean_c,
        rain_sum_mm,

        source AS weather_source,
        source_file AS weather_source_file

    FROM {{ ref('stg_weather_daily') }}

),

final AS (

    SELECT
        COALESCE(o.city_key, w.city_key)
            AS city_key,

        COALESCE(o.report_date, w.report_date)
            AS report_date,

        -- Order metrics
        COALESCE(o.order_count, 0)
            AS order_count,

        COALESCE(
            o.total_order_amount,
            CAST(0 AS NUMERIC)
        ) AS total_order_amount,

        SAFE_DIVIDE(
            o.total_order_amount,
            CAST(o.order_count AS NUMERIC)
        ) AS avg_order_value,

        COALESCE(o.paid_order_count, 0)
            AS paid_order_count,

        COALESCE(o.shipped_order_count, 0)
            AS shipped_order_count,

        COALESCE(o.delivered_order_count, 0)
            AS delivered_order_count,

        -- Weather metrics: keep missing values NULL
        w.temperature_mean_c,
        w.rain_sum_mm,

        w.weather_source,
        w.weather_source_file,

        -- Presence flags
        (o.order_count IS NOT NULL)
            AS has_orders,

        (w.report_date IS NOT NULL)
            AS has_weather,

        CASE
            WHEN w.report_date IS NULL
                OR w.rain_sum_mm IS NULL
                THEN 'unknown'

            WHEN w.rain_sum_mm > 0
                THEN 'rainy'

            WHEN w.rain_sum_mm = 0
                THEN 'dry'

            ELSE 'unknown'
        END AS weather_condition

    FROM orders_city_day AS o

    FULL OUTER JOIN weather_city_day AS w
        ON o.city_key = w.city_key
        AND o.report_date = w.report_date

)

SELECT *
FROM final

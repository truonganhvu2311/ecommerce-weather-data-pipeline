
WITH orders AS (

    SELECT *
    FROM {{ ref('fct_orders') }}

),

weather AS (

    SELECT
        city,
        weather_date,
        temperature_mean_c,
        rain_sum_mm,
        source_file
    FROM {{ ref('stg_weather_daily') }}

),

joined AS (

    SELECT
        o.*,

        w.weather_date,
        w.temperature_mean_c,
        w.rain_sum_mm,
        w.source_file AS weather_source_file,

        CASE
            WHEN w.weather_date IS NULL
              OR w.rain_sum_mm IS NULL
                THEN 'unknown'

            WHEN w.rain_sum_mm > 0
                THEN 'rainy'

            ELSE 'dry'
        END AS weather_condition

    FROM orders AS o

    LEFT JOIN weather AS w
        ON LOWER(TRIM(o.city)) =
           LOWER(TRIM(w.city))

       AND o.order_date_vn = w.weather_date

)

SELECT *
FROM joined


SELECT
    city,
    weather_date,
    COUNT(*) AS record_count

FROM {{ ref('stg_weather_daily') }}

GROUP BY
    city,
    weather_date

HAVING COUNT(*) > 1

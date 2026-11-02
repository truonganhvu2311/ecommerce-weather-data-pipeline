
SELECT
    LOWER(TRIM(city)) AS city_key,
    weather_date,
    COUNT(*) AS duplicate_count

FROM {{ ref('stg_weather_daily') }}

GROUP BY
    LOWER(TRIM(city)),
    weather_date

HAVING COUNT(*) > 1

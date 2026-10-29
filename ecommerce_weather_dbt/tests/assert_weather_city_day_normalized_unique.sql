
SELECT
    LOWER(BTRIM(city)) AS city_key,
    weather_date,
    COUNT(*) AS duplicate_count

FROM {{ ref('stg_weather_daily') }}

GROUP BY
    LOWER(BTRIM(city)),
    weather_date

HAVING COUNT(*) > 1

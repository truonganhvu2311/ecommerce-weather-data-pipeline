
SELECT
    w.city,
    w.weather_date

FROM {{ ref('stg_weather_daily') }} w

WHERE NOT EXISTS (

    SELECT 1

    FROM {{ ref('mart_city_day') }} m

    WHERE m.city_key = NULLIF(
        LOWER(TRIM(w.city)), ''
    )

      AND m.report_date = w.weather_date
      AND m.has_weather IS TRUE

)

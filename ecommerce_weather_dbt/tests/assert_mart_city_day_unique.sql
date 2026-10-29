
SELECT
    city_key,
    report_date,
    COUNT(*) AS duplicate_count

FROM {{ ref('mart_city_day') }}

GROUP BY
    city_key,
    report_date

HAVING COUNT(*) > 1

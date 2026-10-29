
SELECT
    product_id,
    price

FROM {{ ref('stg_products') }}

WHERE price < 0

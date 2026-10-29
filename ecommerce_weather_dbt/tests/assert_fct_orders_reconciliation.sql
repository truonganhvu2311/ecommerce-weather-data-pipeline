
WITH expected AS (

    SELECT
        order_id,
        total_amount
    FROM {{ ref('int_orders_eligible') }}

),

actual AS (

    SELECT
        order_id,
        total_amount
    FROM {{ ref('fct_orders') }}

)

SELECT
    COALESCE(e.order_id, a.order_id) AS order_id,
    e.total_amount AS expected_amount,
    a.total_amount AS actual_amount

FROM expected e

FULL OUTER JOIN actual a
    ON e.order_id = a.order_id

WHERE
    e.order_id IS NULL
    OR a.order_id IS NULL
    OR e.total_amount IS DISTINCT FROM a.total_amount


WITH order_items AS (

    SELECT *
    FROM {{ ref('stg_order_items') }}

),

eligible_orders AS (

    SELECT
        order_id
    FROM {{ ref('fct_orders') }}

),

final AS (

    SELECT
        i.*

    FROM order_items AS i

    INNER JOIN eligible_orders AS o
        ON i.order_id = o.order_id

)

SELECT *
FROM final

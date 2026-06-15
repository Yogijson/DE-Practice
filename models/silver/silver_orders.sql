{{ config(materialized='table') }}

SELECT
    id,
    order_id,
    try_cast(amount AS DOUBLE)        AS amount,
    upper(status)                      AS status,
    order_date,
    ingested_at,
    try_cast(amount AS DOUBLE) IS NOT NULL
        AND upper(status) IN ('COMPLETED', 'PENDING', 'REFUNDED') AS is_valid
FROM {{ ref('bronze_orders') }}
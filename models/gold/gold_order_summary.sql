{{ config(materialized='table') }}

SELECT
    status,
    order_date,
    COUNT(*)        AS order_count,
    ROUND(SUM(amount), 2) AS total_revenue
FROM {{ ref('silver_orders') }}
WHERE is_valid = TRUE
GROUP BY status, order_date
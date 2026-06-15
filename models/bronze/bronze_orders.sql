{{ config(materialized='incremental', unique_key='id') }}

SELECT
    id,
    order_id,
    amount,
    status,
    order_date,
    ingested_at,
    source_system
FROM workspace.default.bronze_orders

{% if is_incremental() %}
    WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
{% endif %}
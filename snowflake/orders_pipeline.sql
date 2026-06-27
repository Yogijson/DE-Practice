-- Step 1: Create a dedicated warehouse for our work
CREATE WAREHOUSE IF NOT EXISTS dev_wh
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60        -- suspend after 60 seconds idle
    AUTO_RESUME = TRUE       -- resume automatically on query
    COMMENT = 'Dev warehouse for learning';

-- Step 2: Create a database
CREATE DATABASE IF NOT EXISTS orders_db;

-- Step 3: Create schemas matching our lakehouse layers
CREATE SCHEMA IF NOT EXISTS orders_db.bronze;
CREATE SCHEMA IF NOT EXISTS orders_db.silver;
CREATE SCHEMA IF NOT EXISTS orders_db.gold;

-- Step 4: Verify
SHOW WAREHOUSES;
SHOW DATABASES;


-- Set context
USE WAREHOUSE dev_wh;
USE DATABASE orders_db;
USE SCHEMA bronze;

-- Create table
CREATE OR REPLACE TABLE bronze.orders (
    id          NUMBER,
    order_id    VARCHAR(50),
    amount      VARCHAR(20),    -- intentionally VARCHAR like raw source
    status      VARCHAR(20),
    order_date  DATE,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(50) DEFAULT 'orders_api'
);

-- Insert raw data (simulating source ingestion)
INSERT INTO bronze.orders (id, order_id, amount, status, order_date)
VALUES
    (1, 'order_001', '500.0',  'completed', '2026-01-15'),
    (2, 'order_002', '250.0',  'pending',   '2026-01-15'),
    (3, 'order_003', NULL,     'completed', '2026-01-15'),
    (4, 'order_004', '750.0',  'COMPLETED', '2026-01-15'),
    (5, 'order_005', '5OO.0',  'completed', '2026-01-15');

SELECT * FROM bronze.orders;

SELECT id, amount, 
       TRY_TO_DOUBLE(amount) AS amount_as_number,
       CASE WHEN TRY_TO_DOUBLE(amount) IS NULL THEN 'BAD DATA' 
            ELSE 'VALID' END AS data_quality
FROM bronze.orders;

-- Silver layer
CREATE OR REPLACE TABLE silver.orders AS
SELECT
    id,
    order_id,
    TRY_TO_DOUBLE(amount)                    AS amount,
    UPPER(status)                             AS status,
    order_date,
    ingested_at,
    TRY_TO_DOUBLE(amount) IS NOT NULL
        AND UPPER(status) IN ('COMPLETED','PENDING','REFUNDED') AS is_valid
FROM bronze.orders;

SELECT * FROM silver.orders;

-- Gold layer
CREATE OR REPLACE TABLE gold.order_summary AS
SELECT
    status,
    order_date,
    COUNT(*)            AS order_count,
    ROUND(SUM(amount), 2) AS total_revenue
FROM silver.orders
WHERE is_valid = TRUE
GROUP BY status, order_date;

SELECT * FROM gold.order_summary;

-- Drop the static version
DROP TABLE IF EXISTS silver.orders;

-- Rebuild as Dynamic Table
CREATE OR REPLACE DYNAMIC TABLE silver.orders
    TARGET_LAG = '1 minute'
    WAREHOUSE = dev_wh
AS
SELECT
    id,
    order_id,
    TRY_TO_DOUBLE(amount)                        AS amount,
    UPPER(status)                                 AS status,
    order_date,
    ingested_at,
    TRY_TO_DOUBLE(amount) IS NOT NULL
        AND UPPER(status) IN ('COMPLETED','PENDING','REFUNDED') AS is_valid
FROM bronze.orders;

-- Insert new Bronze row
INSERT INTO bronze.orders (id, order_id, amount, status, order_date)
VALUES (6, 'order_006', '900.0', 'completed', '2026-01-16');

-- Wait 60 seconds, then check Silver
SELECT * FROM silver.orders ORDER BY id;

-- Query Bronze as it was before row 6 was inserted
SELECT * FROM bronze.orders
    BEFORE(STATEMENT => LAST_QUERY_ID(-2));

-- Or by timestamp
SELECT * FROM bronze.orders
    AT(TIMESTAMP => DATEADD(minutes, -5, CURRENT_TIMESTAMP()));

-- Or by offset in seconds
SELECT * FROM bronze.orders
    AT(OFFSET => -300);  -- 5 minutes ago

-- Clone entire database for testing — instant, free
CREATE DATABASE orders_db_dev CLONE orders_db;

-- Verify clone has same data
SELECT * FROM orders_db_dev.bronze.orders;

-- Now you can safely test destructive operations on the clone
DELETE FROM orders_db_dev.bronze.orders WHERE id = 6;

-- Original is untouched
SELECT COUNT(*) FROM orders_db.bronze.orders;     -- still 6
SELECT COUNT(*) FROM orders_db_dev.bronze.orders; -- now 5

CREATE or replace DATABASE orders_db_dev CLONE orders_db;

SELECT COUNT(*) FROM orders_db_dev.bronze.orders;   -- should be 6

DELETE FROM orders_db_dev.bronze.orders WHERE id = 6;

-- Verify isolation
SELECT COUNT(*) FROM orders_db.bronze.orders;        -- should still be 6
SELECT COUNT(*) FROM orders_db_dev.bronze.orders;    -- should be 5


-- Create a stream on Bronze to track changes
CREATE OR REPLACE STREAM bronze.orders_stream 
ON TABLE bronze.orders;

-- Check stream — currently empty (no changes since stream created)
SELECT * FROM bronze.orders_stream;

INSERT INTO bronze.orders (id, order_id, amount, status, order_date)
VALUES (7, 'order_007', '300.0', 'pending', '2026-01-17');

-- Now check the stream — it captured the change
SELECT * FROM bronze.orders_stream;

-- Task that consumes the stream and merges into Silver
CREATE OR REPLACE TASK silver.refresh_orders_task
    WAREHOUSE = dev_wh
    SCHEDULE = '1 MINUTE'
WHEN
    SYSTEM$STREAM_HAS_DATA('bronze.orders_stream')
AS
MERGE INTO silver.orders t
USING (
    SELECT
        id,
        order_id,
        TRY_TO_DOUBLE(amount)     AS amount,
        UPPER(status)              AS status,
        order_date,
        ingested_at,
        TRY_TO_DOUBLE(amount) IS NOT NULL
            AND UPPER(status) IN ('COMPLETED','PENDING','REFUNDED') AS is_valid
    FROM bronze.orders_stream
    WHERE METADATA$ACTION = 'INSERT'
) s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET
    t.amount   = s.amount,
    t.status   = s.status,
    t.is_valid = s.is_valid
WHEN NOT MATCHED THEN INSERT
    (id, order_id, amount, status, order_date, ingested_at, is_valid)
VALUES
    (s.id, s.order_id, s.amount, s.status, s.order_date, s.ingested_at, s.is_valid);

-- Tasks are created in suspended state -- must resume manually
ALTER TASK silver.refresh_orders_task RESUME;

-- Check task status
SHOW TASKS IN SCHEMA silver;

-- Did the task run?
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'refresh_orders_task'
)) ORDER BY scheduled_time DESC;

-- Did Silver get row 7?
SELECT * FROM silver.orders ORDER BY id;

-- Drop the Dynamic Table
DROP TABLE IF EXISTS silver.orders;

-- Recreate as regular table
CREATE OR REPLACE TABLE silver.orders AS
SELECT
    id,
    order_id,
    TRY_TO_DOUBLE(amount)     AS amount,
    UPPER(status)              AS status,
    order_date,
    ingested_at,
    TRY_TO_DOUBLE(amount) IS NOT NULL
        AND UPPER(status) IN ('COMPLETED','PENDING','REFUNDED') AS is_valid
FROM bronze.orders;

-- Recreate the stream (dropping Dynamic Table may have invalidated it)
CREATE OR REPLACE STREAM bronze.orders_stream
ON TABLE bronze.orders;

-- Insert a new row to give the stream something to process
INSERT INTO bronze.orders (id, order_id, amount, status, order_date)
VALUES (8, 'order_008', '400.0', 'completed', '2026-01-18');
USE ROLE ACCOUNTADMIN;
USE DATABASE AI_FOR_GOOD_DB;
USE SCHEMA STOCK_GUARDIAN;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE STREAM DAILY_STOCK_STREAM
ON TABLE DAILY_STOCK
APPEND_ONLY = TRUE;

SELECT * FROM DAILY_STOCK_STREAM;

CREATE OR REPLACE TABLE PIPELINE_LOG (
    run_time TIMESTAMP,
    message STRING
);

CREATE OR REPLACE TASK SIMULATE_DAILY_STOCK_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 */2 * * * UTC'  -- runs every 2 hours
AS
BEGIN

-- Insert a new simulated day (simple logic)
INSERT INTO DAILY_STOCK
SELECT
    DATEADD(day, 1, MAX(date)),
    location_id,
    location_name,
    item_id,
    item_name,
    closing_stock,
    0 AS received,
    ROUND(avg_daily_issue * UNIFORM(0.8, 1.2, RANDOM())),
    GREATEST(
        0,
        closing_stock - ROUND(avg_daily_issue * UNIFORM(0.8, 1.2, RANDOM()))
    ),
    lead_time_days
FROM FAIR_STOCK_PRIORITY
GROUP BY
    location_id, location_name, item_id, item_name,
    closing_stock, avg_daily_issue, lead_time_days;

-- Log run
INSERT INTO PIPELINE_LOG
VALUES (CURRENT_TIMESTAMP(), 'Daily stock simulation executed');

END;

ALTER TASK SIMULATE_DAILY_STOCK_TASK RESUME;


SHOW TASKS;

SELECT *
FROM DAILY_STOCK
ORDER BY date DESC;

SELECT * FROM DAILY_STOCK_STREAM;

SELECT *
FROM FAIR_STOCK_PRIORITY
ORDER BY fair_priority_score DESC;


SELECT *
FROM PIPELINE_LOG
ORDER BY run_time DESC;

EXECUTE TASK SIMULATE_DAILY_STOCK_TASK;

SELECT * FROM PIPELINE_LOG ORDER BY run_time DESC;
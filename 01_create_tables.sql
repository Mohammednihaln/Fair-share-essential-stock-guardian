CREATE DATABASE AI_FOR_GOOD_DB;
USE DATABASE AI_FOR_GOOD_DB;
CREATE SCHEMA STOCK_GUARDIAN;
USE SCHEMA STOCK_GUARDIAN;
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE TABLE DAILY_STOCK (
    date DATE,
    location_id STRING,
    location_name STRING,
    item_id STRING,
    item_name STRING,
    opening_stock NUMBER,
    received NUMBER,
    issued NUMBER,
    closing_stock NUMBER,
    lead_time_days NUMBER
);

SHOW TABLES;

CREATE OR REPLACE TABLE LOCATION_VULNERABILITY (
    location_id STRING,
    population NUMBER,
    under5_ratio FLOAT,
    elderly_ratio FLOAT,
    poverty_index FLOAT,
    distance_to_hospital_km FLOAT
);

INSERT INTO LOCATION_VULNERABILITY VALUES
('L1', 500000, 0.18, 0.07, 0.8, 25),
('L2', 350000, 0.12, 0.05, 0.4, 10),
('L3', 800000, 0.22, 0.09, 0.9, 40),
('L4', 600000, 0.10, 0.06, 0.3, 5),
('L5', 450000, 0.15, 0.08, 0.6, 18);

INSERT INTO DAILY_STOCK VALUES
('2025-01-01','L1','District A','I1','Paracetamol',500,50,80,470,7),
('2025-01-01','L2','District B','I1','Paracetamol',800,100,40,860,7),
('2025-01-01','L3','District C','I1','Paracetamol',300,20,90,230,7),
('2025-01-01','L4','District D','I1','Paracetamol',1000,0,30,970,7),
('2025-01-01','L5','District E','I1','Paracetamol',400,30,70,360,7);


SELECT
  location_name,
  item_name,
  closing_stock,
  lead_time_days
FROM DAILY_STOCK;

SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();


SELECT
    date,
    location_id,
    location_name,
    item_id,
    item_name,
    closing_stock,
    lead_time_days,

    -- 7-day rolling average consumption
    AVG(issued) OVER (
        PARTITION BY location_id, item_id
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS avg_daily_issue,

    -- Days to stockout
    closing_stock /
      NULLIF(
        AVG(issued) OVER (
            PARTITION BY location_id, item_id
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 0
      ) AS days_to_stockout

FROM DAILY_STOCK
ORDER BY location_id, item_id, date;


SELECT
    date,
    location_id,
    location_name,
    item_id,
    item_name,
    closing_stock,
    lead_time_days,

    AVG(issued) OVER (
        PARTITION BY location_id, item_id
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS avg_daily_issue,

    closing_stock /
      NULLIF(
        AVG(issued) OVER (
            PARTITION BY location_id, item_id
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 0
      ) AS days_to_stockout,

    CASE
        WHEN
          closing_stock /
          NULLIF(
            AVG(issued) OVER (
                PARTITION BY location_id, item_id
                ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 0
          ) < lead_time_days
        THEN 'HIGH'
        ELSE 'OK'
    END AS risk_flag

FROM DAILY_STOCK
ORDER BY location_id, item_id, date;
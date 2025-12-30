CREATE OR REPLACE DYNAMIC TABLE STOCK_HEALTH
TARGET_LAG = '1 minute'
WAREHOUSE = COMPUTE_WH
AS
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

FROM DAILY_STOCK;

SHOW DYNAMIC TABLES;

SELECT * FROM STOCK_HEALTH;

SELECT COUNT(*) FROM STOCK_HEALTH;
SELECT COUNT(*) FROM LOCATION_VULNERABILITY;

SELECT
    sh.date,
    sh.location_id,
    sh.location_name,
    sh.item_name,
    sh.closing_stock,
    sh.days_to_stockout,
    lv.population,
    lv.under5_ratio,
    lv.elderly_ratio,
    lv.poverty_index,
    lv.distance_to_hospital_km
FROM STOCK_HEALTH sh
JOIN LOCATION_VULNERABILITY lv
  ON sh.location_id = lv.location_id;


  SELECT
    location_id,
    poverty_index,
    under5_ratio,
    elderly_ratio,

    distance_to_hospital_km,
    distance_to_hospital_km
      / MAX(distance_to_hospital_km) OVER () AS norm_distance
FROM LOCATION_VULNERABILITY;


SELECT
    location_id,

    (
      0.4 * poverty_index +
      0.3 * under5_ratio +
      0.2 * elderly_ratio +
      0.1 * (
        distance_to_hospital_km
        / MAX(distance_to_hospital_km) OVER ()
      )
    ) AS vulnerability_score
FROM LOCATION_VULNERABILITY;


SELECT
    sh.date,
    sh.location_name,
    sh.item_name,
    sh.days_to_stockout,

    (
      0.4 * lv.poverty_index +
      0.3 * lv.under5_ratio +
      0.2 * lv.elderly_ratio +
      0.1 * (
        lv.distance_to_hospital_km
        / MAX(lv.distance_to_hospital_km) OVER ()
      )
    ) AS vulnerability_score,

    (
      (
        0.4 * lv.poverty_index +
        0.3 * lv.under5_ratio +
        0.2 * lv.elderly_ratio +
        0.1 * (
          lv.distance_to_hospital_km
          / MAX(lv.distance_to_hospital_km) OVER ()
        )
      )
      /
      NULLIF(sh.days_to_stockout, 0)
    ) AS fair_priority_score

FROM STOCK_HEALTH sh
JOIN LOCATION_VULNERABILITY lv
  ON sh.location_id = lv.location_id
ORDER BY fair_priority_score DESC;


CREATE OR REPLACE DYNAMIC TABLE FAIR_STOCK_PRIORITY
TARGET_LAG = '1 minute'
WAREHOUSE = COMPUTE_WH
AS
SELECT
    sh.date,
    sh.location_id,
    sh.location_name,
    sh.item_id,
    sh.item_name,
    sh.closing_stock,
    sh.avg_daily_issue,
    sh.days_to_stockout,
    sh.risk_flag,

    (
      0.4 * lv.poverty_index +
      0.3 * lv.under5_ratio +
      0.2 * lv.elderly_ratio +
      0.1 * (
        lv.distance_to_hospital_km
        / MAX(lv.distance_to_hospital_km) OVER ()
      )
    ) AS vulnerability_score,

    (
      (
        0.4 * lv.poverty_index +
        0.3 * lv.under5_ratio +
        0.2 * lv.elderly_ratio +
        0.1 * (
          lv.distance_to_hospital_km
          / MAX(lv.distance_to_hospital_km) OVER ()
        )
      )
      /
      NULLIF(sh.days_to_stockout, 0)
    ) AS fair_priority_score

FROM STOCK_HEALTH sh
JOIN LOCATION_VULNERABILITY lv
  ON sh.location_id = lv.location_id;




SELECT
  location_name,
  item_name,
  days_to_stockout,
  vulnerability_score,
  fair_priority_score
FROM FAIR_STOCK_PRIORITY
ORDER BY fair_priority_score DESC;


SELECT COUNT(*) FROM FAIR_STOCK_PRIORITY;


SELECT COUNT(*) FROM FAIR_STOCK_PRIORITY;


SELECT
    location_name,
    item_name,
    closing_stock,
    avg_daily_issue,
    days_to_stockout,
    risk_flag,

    GREATEST(
      0,
      lead_time_days * avg_daily_issue - closing_stock
    ) AS recommended_reorder_qty

FROM FAIR_STOCK_PRIORITY
WHERE risk_flag = 'HIGH'
ORDER BY recommended_reorder_qty DESC;


CREATE OR REPLACE DYNAMIC TABLE FAIR_STOCK_PRIORITY
TARGET_LAG = '1 minute'
WAREHOUSE = COMPUTE_WH
AS
SELECT
    sh.date,
    sh.location_id,
    sh.location_name,
    sh.item_id,
    sh.item_name,
    sh.closing_stock,
    sh.lead_time_days,        -- ✅ FIX: added
    sh.avg_daily_issue,
    sh.days_to_stockout,
    sh.risk_flag,

    (
      0.4 * lv.poverty_index +
      0.3 * lv.under5_ratio +
      0.2 * lv.elderly_ratio +
      0.1 * (
        lv.distance_to_hospital_km
        / MAX(lv.distance_to_hospital_km) OVER ()
      )
    ) AS vulnerability_score,

    (
      (
        0.4 * lv.poverty_index +
        0.3 * lv.under5_ratio +
        0.2 * lv.elderly_ratio +
        0.1 * (
          lv.distance_to_hospital_km
          / MAX(lv.distance_to_hospital_km) OVER ()
        )
      )
      /
      NULLIF(sh.days_to_stockout, 0)
    ) AS fair_priority_score

FROM STOCK_HEALTH sh
JOIN LOCATION_VULNERABILITY lv
  ON sh.location_id = lv.location_id;

  DESC TABLE FAIR_STOCK_PRIORITY;


  
SELECT
    location_name,
    item_name,
    closing_stock,
    avg_daily_issue,
    days_to_stockout,
    risk_flag,

    GREATEST(
      0,
      lead_time_days * avg_daily_issue - closing_stock
    ) AS recommended_reorder_qty

FROM FAIR_STOCK_PRIORITY
WHERE risk_flag = 'HIGH'
ORDER BY recommended_reorder_qty DESC;


SELECT
    location_name,
    item_name,
    closing_stock,
    days_to_stockout
FROM FAIR_STOCK_PRIORITY
WHERE days_to_stockout > 10
ORDER BY days_to_stockout DESC;


SELECT
    location_name,
    item_name,
    closing_stock,
    days_to_stockout,
    fair_priority_score
FROM FAIR_STOCK_PRIORITY
WHERE risk_flag = 'HIGH'
ORDER BY fair_priority_score DESC;


SELECT
    r.item_name,

    r.location_name AS receiver_location,
    r.days_to_stockout AS receiver_days_left,
    r.fair_priority_score,

    d.location_name AS donor_location,
    d.days_to_stockout AS donor_days_left,

    LEAST(
      d.closing_stock * 0.3,
      r.lead_time_days * r.avg_daily_issue
    ) AS suggested_transfer_qty

FROM FAIR_STOCK_PRIORITY r
JOIN FAIR_STOCK_PRIORITY d
  ON r.item_id = d.item_id
 AND r.location_id <> d.location_id

WHERE
    r.risk_flag = 'HIGH'
AND d.days_to_stockout > 10

ORDER BY r.fair_priority_score DESC;
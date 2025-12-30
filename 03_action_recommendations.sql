CREATE OR REPLACE TABLE ACTION_RECOMMENDATIONS AS
SELECT
    date,
    location_id,
    location_name,
    item_id,
    item_name,
    'REORDER' AS action_type,

    GREATEST(
      0,
      lead_time_days * avg_daily_issue - closing_stock
    ) AS action_quantity,

    'Stock projected to run out before next replenishment' AS action_reason

FROM FAIR_STOCK_PRIORITY
WHERE risk_flag = 'HIGH'

UNION ALL

SELECT
    r.date,
    r.location_id,
    r.location_name,
    r.item_id,
    r.item_name,
    'REDISTRIBUTE' AS action_type,

    LEAST(
      d.closing_stock * 0.3,
      r.lead_time_days * r.avg_daily_issue
    ) AS action_quantity,

    CONCAT(
      'Transfer from ',
      d.location_name,
      ' to prevent stockout in high-vulnerability area'
    ) AS action_reason

FROM FAIR_STOCK_PRIORITY r
JOIN FAIR_STOCK_PRIORITY d
  ON r.item_id = d.item_id
 AND r.location_id <> d.location_id

WHERE
    r.risk_flag = 'HIGH'
AND d.days_to_stockout > 10;




SELECT *
FROM ACTION_RECOMMENDATIONS
ORDER BY action_type, action_quantity DESC;
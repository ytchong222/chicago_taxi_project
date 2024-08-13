{{ config(materialized='table') }}

SELECT
  taxi_id,
  SUM(revenue) AS total_revenue,
  AVG(revenue) AS avg_revenue_per_trip
FROM
  `chicago_taxi_trips.taxi_trips`
GROUP BY
  taxi_id
ORDER BY
  total_revenue DESC
LIMIT 100


-- Use the `ref` function to select from other models
{{ config(materialized='table') }}
WITH recent_data AS (
    SELECT
        taxi_id,
        total_fare,
        total_tips,
        total_extra_payment,
		total_tolls
    FROM
        dbt_taxi_trips.taxi_trips_summary
    WHERE
        DATE(tdate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
),
aggregated_earnings AS (
    SELECT
        taxi_id,
        SUM(total_fare) AS total_fare,
        SUM(total_tips) AS total_tips,
        SUM(total_extra_payment) AS total_extra_payment,
        SUM(total_fare + total_tips + total_extra_payment-total_tolls) AS total_earnings
    FROM
        recent_data
    GROUP BY
        taxi_id
)
SELECT
    taxi_id,
    total_fare,
    total_tips,
    total_extra_payment,
    total_earnings
FROM
    aggregated_earnings
ORDER BY
    total_earnings DESC
LIMIT 100

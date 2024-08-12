-- models/top_tips_last_3months.sql
{{ config(materialized='table') }}
WITH top_tips_last_3months AS (
    SELECT
        taxi_id,
        SUM(tips) AS total_tips
    FROM
        `chicago_taxi_trips.taxi_trips`
    WHERE
         DATE(trip_end_timestamp) >= DATE_SUB(DATE('2024-01-02'), INTERVAL 3 MONTH)
    GROUP BY
        taxi_id
)
SELECT
    taxi_id,
    total_tips
FROM
    top_tips_last_3months
ORDER BY
    total_tips DESC limit 100

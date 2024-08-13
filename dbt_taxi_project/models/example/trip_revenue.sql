{{ config(materialized='table') }}

WITH trip_revenue AS (
    SELECT
        company,
        taxi_id,
        trip_end_timestamp,
        (fare + tips + extras - tolls) AS total_revenue
    FROM
        `chicago_taxi_trips.taxi_trips`
    WHERE
        FORMAT_TIMESTAMP('%Y-%m-%d', trip_end_timestamp) BETWEEN '2023-06-01' AND '2024-01-02'
)

SELECT
    FORMAT_DATE('%Y-%m-%d', LAST_DAY(CAST(trip_end_timestamp AS DATE))) AS monthdate,
    company,
    COUNT(distinct taxi_id ) AS no_taxi,
    COUNT(*) AS total_trips,
    SUM(total_revenue) AS total_revenue_generated,
    AVG(total_revenue) AS avg_revenue_per_trip
FROM
    trip_revenue
GROUP BY
    company,
    monthdate
ORDER BY
    total_revenue_generated DESC


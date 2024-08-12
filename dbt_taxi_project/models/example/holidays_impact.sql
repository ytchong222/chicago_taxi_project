-- models/holiday_impact.sql
WITH trip_analysis AS (
    SELECT
        FORMAT_TIMESTAMP('%Y-%m-%d', trip_end_timestamp) AS tdate,
        COUNT(*) AS total_trips,
        SUM(trip_total) AS total_revenue
    FROM
         `chicago_taxi_trips.taxi_trips`
    GROUP BY
        tdate
)
SELECT
    ta.tdate,
    ta.total_trips,
    ta.total_revenue,
    d.holiday_name
FROM
    trip_analysis ta
left JOIN
    `chicago_taxi_trips.dbt_taxi_trips` d ON cast(ta.tdate as date) = d.holiday_date
ORDER BY
    tdate desc limit 100

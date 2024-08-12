-- models/top_tips_last_3months.sql
WITH top_tips_last_3months AS (
    SELECT
        taxi_id,
        SUM(tips) AS total_tips
    FROM
        `chicago_taxi_trips.taxi_trips`
    WHERE
         FORMAT_TIMESTAMP('%Y-%m-%d', trip_end_timestamp)  >= DATE_SUB(2024-01-02, INTERVAL 3 MONTH)
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

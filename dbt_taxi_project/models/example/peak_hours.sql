{{ config(materialized='table') }}


WITH day_hour_counts AS (
  SELECT
    EXTRACT(DAYOFWEEK FROM trip_start_timestamp) AS day_of_week_num,
    EXTRACT(HOUR FROM trip_start_timestamp) AS hour_of_day,
    COUNT(*) AS number_of_trips
  FROM
    `chicago_taxi_trips.taxi_trips`
  WHERE
    FORMAT_TIMESTAMP('%Y-%m-%d', trip_start_timestamp) BETWEEN '2023-10-01' AND '2024-01-02'
  GROUP BY
    day_of_week_num,
    hour_of_day
)
SELECT
  CASE
    WHEN day_of_week_num = 1 THEN 'Sunday'
    WHEN day_of_week_num = 2 THEN 'Monday'
    WHEN day_of_week_num = 3 THEN 'Tuesday'
    WHEN day_of_week_num = 4 THEN 'Wednesday'
    WHEN day_of_week_num = 5 THEN 'Thursday'
    WHEN day_of_week_num = 6 THEN 'Friday'
    WHEN day_of_week_num = 7 THEN 'Saturday'
    ELSE 'NA'
  END AS day_of_week,
  hour_of_day,
  number_of_trips
FROM
  day_hour_counts
ORDER BY
  number_of_trips DESC


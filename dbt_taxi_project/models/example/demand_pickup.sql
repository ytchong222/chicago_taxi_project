SELECT
  pickup_location,
  COUNT(*) AS number_of_trips
FROM
  `chicago_taxi_trips.taxi_trips`
GROUP BY
  pickup_location
ORDER BY
  number_of_trips DESC
LIMIT 100

-- ST5011CEM secure parameterised query examples
-- Values must be supplied separately through cursor.execute().
-- Never concatenate user-controlled values into SQL.

-- Query 1: route summary
SELECT
    line_ref,
    COUNT(*) AS segment_records,
    ROUND(AVG(runtime_minutes), 4) AS average_runtime_minutes,
    ROUND(AVG(distance_km), 4) AS average_distance_km,
    SUM(is_iqr_high_duration) AS high_duration_records
FROM segments
WHERE line_ref = ?
GROUP BY line_ref;

-- Query 2: model anomalies above a selected error threshold
SELECT
    line_ref,
    from_stop_ref,
    to_stop_ref,
    actual_runtime_minutes,
    predicted_runtime_minutes,
    absolute_error_minutes,
    anomaly_direction
FROM predictions
WHERE is_model_anomaly = ?
  AND absolute_error_minutes >= ?
ORDER BY absolute_error_minutes DESC
LIMIT ?;

-- Query 3: segments inside a distance range
SELECT
    line_ref,
    from_stop_ref,
    to_stop_ref,
    distance_km,
    runtime_minutes
FROM segments
WHERE distance_km BETWEEN ? AND ?
ORDER BY distance_km DESC
LIMIT ?;

-- Query 4: exact route lookup used for the injection test
SELECT COUNT(*)
FROM segments
WHERE line_ref = ?;

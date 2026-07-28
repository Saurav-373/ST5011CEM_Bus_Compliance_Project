PRAGMA foreign_keys = ON;

CREATE TABLE stops (
    stop_ref TEXT PRIMARY KEY,
    stop_name TEXT NOT NULL,
    locality_name TEXT,
    town TEXT,
    latitude REAL NOT NULL
        CHECK (latitude BETWEEN -90.0 AND 90.0),
    longitude REAL NOT NULL
        CHECK (longitude BETWEEN -180.0 AND 180.0),
    stop_type TEXT,
    status TEXT,
    administrative_area_code TEXT,
    atco_area_code TEXT
);

CREATE TABLE model_results (
    model_name TEXT PRIMARY KEY,
    rank_by_rmse INTEGER NOT NULL
        CHECK (rank_by_rmse >= 1),
    configuration TEXT NOT NULL,
    rmse REAL NOT NULL CHECK (rmse >= 0),
    mae REAL NOT NULL CHECK (mae >= 0),
    r2 REAL NOT NULL,
    training_seconds REAL NOT NULL
        CHECK (training_seconds >= 0),
    evaluation_seconds REAL NOT NULL
        CHECK (evaluation_seconds >= 0)
);

CREATE TABLE segments (
    segment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT NOT NULL,
    vehicle_journey_code TEXT NOT NULL,
    vehicle_timing_link_id TEXT,
    line_ref TEXT NOT NULL,
    segment_key TEXT NOT NULL,
    departure_time TEXT NOT NULL,
    departure_hour INTEGER NOT NULL
        CHECK (departure_hour BETWEEN 0 AND 23),
    time_of_day TEXT NOT NULL,
    is_peak_hour INTEGER NOT NULL
        CHECK (is_peak_hour IN (0, 1)),
    from_stop_ref TEXT NOT NULL,
    to_stop_ref TEXT NOT NULL,
    distance_km REAL NOT NULL CHECK (distance_km >= 0),
    runtime_minutes REAL NOT NULL CHECK (runtime_minutes > 0),
    is_iqr_high_duration INTEGER NOT NULL
        CHECK (is_iqr_high_duration IN (0, 1)),
    FOREIGN KEY (from_stop_ref) REFERENCES stops(stop_ref),
    FOREIGN KEY (to_stop_ref) REFERENCES stops(stop_ref)
);

CREATE TABLE predictions (
    prediction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    model_name TEXT NOT NULL,
    source_file TEXT NOT NULL,
    vehicle_journey_code TEXT NOT NULL,
    line_ref TEXT NOT NULL,
    segment_key TEXT NOT NULL,
    departure_time TEXT NOT NULL,
    from_stop_ref TEXT NOT NULL,
    to_stop_ref TEXT NOT NULL,
    distance_km REAL NOT NULL CHECK (distance_km >= 0),
    actual_runtime_minutes REAL NOT NULL
        CHECK (actual_runtime_minutes > 0),
    predicted_runtime_minutes REAL NOT NULL,
    residual_minutes REAL NOT NULL,
    absolute_error_minutes REAL NOT NULL
        CHECK (absolute_error_minutes >= 0),
    is_model_anomaly INTEGER NOT NULL
        CHECK (is_model_anomaly IN (0, 1)),
    anomaly_direction TEXT NOT NULL,
    FOREIGN KEY (model_name) REFERENCES model_results(model_name),
    FOREIGN KEY (from_stop_ref) REFERENCES stops(stop_ref),
    FOREIGN KEY (to_stop_ref) REFERENCES stops(stop_ref)
);

CREATE INDEX idx_segments_line_ref
    ON segments(line_ref);

CREATE INDEX idx_segments_segment_key
    ON segments(segment_key);

CREATE INDEX idx_segments_departure_hour
    ON segments(departure_hour);

CREATE INDEX idx_segments_runtime
    ON segments(runtime_minutes);

CREATE INDEX idx_segments_distance
    ON segments(distance_km);

CREATE INDEX idx_segments_origin_stop
    ON segments(from_stop_ref);

CREATE INDEX idx_segments_destination_stop
    ON segments(to_stop_ref);

CREATE INDEX idx_predictions_line_ref
    ON predictions(line_ref);

CREATE INDEX idx_predictions_anomaly
    ON predictions(is_model_anomaly, absolute_error_minutes);

CREATE INDEX idx_predictions_model
    ON predictions(model_name);

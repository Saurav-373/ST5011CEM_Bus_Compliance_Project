-- ST5011CEM sample SQLite export
-- Run against a new empty SQLite database.

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

BEGIN TRANSACTION;
-- Five sample rows from stops
INSERT INTO stops (stop_ref, stop_name, locality_name, town, latitude, longitude, stop_type, status, administrative_area_code, atco_area_code) VALUES ('1400EB0002', 'St Leonard''s Place', 'Old Town', '', 50.7710486, 0.2577982, 'BCT', 'active', '079', '140');
INSERT INTO stops (stop_ref, stop_name, locality_name, town, latitude, longitude, stop_type, status, administrative_area_code, atco_area_code) VALUES ('1400EB0003', 'Recreation Ground', 'Old Town', '', 50.772923, 0.255265, 'BCT', 'active', '079', '140');
INSERT INTO stops (stop_ref, stop_name, locality_name, town, latitude, longitude, stop_type, status, administrative_area_code, atco_area_code) VALUES ('1400EB0005', 'Longland Road', 'Old Town', '', 50.7743572, 0.254142, 'BCT', 'active', '079', '140');
INSERT INTO stops (stop_ref, stop_name, locality_name, town, latitude, longitude, stop_type, status, administrative_area_code, atco_area_code) VALUES ('1400EB0007', 'Osborne Road', 'Old Town', '', 50.7740007, 0.2521107, 'BCT', 'active', '079', '140');
INSERT INTO stops (stop_ref, stop_name, locality_name, town, latitude, longitude, stop_type, status, administrative_area_code, atco_area_code) VALUES ('1400EB0008', 'Osborne Road', 'Old Town', '', 50.7739833, 0.2516134, 'BCT', 'active', '079', '140');
COMMIT;

BEGIN TRANSACTION;
-- Five sample rows from model_results
INSERT INTO model_results (model_name, rank_by_rmse, configuration, rmse, mae, r2, training_seconds, evaluation_seconds) VALUES ('Gradient-Boosted Trees', 1, 'maxIter=20; maxDepth=6; stepSize=0.1; subsamplingRate=0.8', 0.668221, 0.414582, 0.458255, 19.9385, 0.178);
INSERT INTO model_results (model_name, rank_by_rmse, configuration, rmse, mae, r2, training_seconds, evaluation_seconds) VALUES ('Decision Tree', 2, 'maxDepth=10; minInstancesPerNode=20', 0.72233, 0.429214, 0.366968, 3.4168, 0.2196);
INSERT INTO model_results (model_name, rank_by_rmse, configuration, rmse, mae, r2, training_seconds, evaluation_seconds) VALUES ('Linear Regression', 3, 'regParam=0.05; elasticNetParam=0.0; maxIter=50', 0.728005, 0.472387, 0.356983, 1.0321, 0.4381);
INSERT INTO model_results (model_name, rank_by_rmse, configuration, rmse, mae, r2, training_seconds, evaluation_seconds) VALUES ('Random Forest', 4, 'numTrees=40; maxDepth=10; featureSubsetStrategy=sqrt; subsamplingRate=0.8', 0.730687, 0.490756, 0.352236, 15.2687, 0.421);
COMMIT;

BEGIN TRANSACTION;
-- Five sample rows from segments
INSERT INTO segments (segment_id, source_file, vehicle_journey_code, vehicle_timing_link_id, line_ref, segment_key, departure_time, departure_hour, time_of_day, is_peak_hour, from_stop_ref, to_stop_ref, distance_km, runtime_minutes, is_iqr_high_duration) VALUES (1, '319-319--SCEK-SVSE-2026-04-12-Hastings_-_12th_April_2026_-_Copy_o__SCEK_PK0000098_431_20260726-BODS_V1_1.xml', 'VJ619', 'VJTL4020', 'SCEK:PK0000098:431:319', '1400HA0144_1400HA0142', '12:13:00', 12, 'Afternoon', 0, '1400HA0144', '1400HA0142', 0.1626820704766919, 1.0, 0);
INSERT INTO segments (segment_id, source_file, vehicle_journey_code, vehicle_timing_link_id, line_ref, segment_key, departure_time, departure_hour, time_of_day, is_peak_hour, from_stop_ref, to_stop_ref, distance_km, runtime_minutes, is_iqr_high_duration) VALUES (2, 'E001-1--SCEK-EBSE-2026-07-26-EB_260726_Final__SCEK_PK0000098_262_20260726-BODS_V1_1.xml', 'VJ167', 'VJTL1441', 'SCEK:PK0000098:262:1', '1400EB75035_1400EB0318', '11:07:00', 11, 'Morning', 0, '1400EB75035', '1400EB0318', 0.3761963902461093, 1.0, 0);
INSERT INTO segments (segment_id, source_file, vehicle_journey_code, vehicle_timing_link_id, line_ref, segment_key, departure_time, departure_hour, time_of_day, is_peak_hour, from_stop_ref, to_stop_ref, distance_km, runtime_minutes, is_iqr_high_duration) VALUES (3, '329-329--SCEK-SVSE-2026-04-12-Hastings_-_12th_April_2026_-_Copy_o__SCEK_PK0000098_325_20260726-BODS_V1_1.xml', 'VJ687', 'VJTL338', 'SCEK:PK0000098:325:329A', '1400HA0222_1400HA10007', '18:30:00', 18, 'Evening', 1, '1400HA0222', '1400HA10007', 0.36185084927009364, 2.0, 0);
INSERT INTO segments (segment_id, source_file, vehicle_journey_code, vehicle_timing_link_id, line_ref, segment_key, departure_time, departure_hour, time_of_day, is_peak_hour, from_stop_ref, to_stop_ref, distance_km, runtime_minutes, is_iqr_high_duration) VALUES (4, 'LOOP-None--SCEK-THSE-2026-06-07-7_June_2026_with_OTv2__SCEK_PK0000098_146_20260726-BODS_V1_1.xml', 'VJ728', 'VJTL12491', 'SCEK:PK0000098:146:LOOP', '2400A047280A_2400A047290A', '11:55:00', 11, 'Morning', 0, '2400A047280A', '2400A047290A', 0.35494873064300914, 1.0, 0);
INSERT INTO segments (segment_id, source_file, vehicle_journey_code, vehicle_timing_link_id, line_ref, segment_key, departure_time, departure_hour, time_of_day, is_peak_hour, from_stop_ref, to_stop_ref, distance_km, runtime_minutes, is_iqr_high_duration) VALUES (5, '502-502--SCEK-ASSE-2026-07-26-Ashford_-_26th_July_2026_-_Hols_Agr__SCEK_PK0000098_64_20260726-BODS_V1_1.xml', 'VJ998', 'VJTL1623', 'SCEK:PK0000098:64:502', '240099796_240099798', '07:07:00', 7, 'Morning', 1, '240099796', '240099798', 0.3756900468757805, 1.0, 0);
COMMIT;

BEGIN TRANSACTION;
-- Five sample rows from predictions
INSERT INTO predictions (prediction_id, model_name, source_file, vehicle_journey_code, line_ref, segment_key, departure_time, from_stop_ref, to_stop_ref, distance_km, actual_runtime_minutes, predicted_runtime_minutes, residual_minutes, absolute_error_minutes, is_model_anomaly, anomaly_direction) VALUES (1, 'Gradient-Boosted Trees', '502-502--SCEK-ASSE-2026-07-26-Ashford_-_26th_July_2026_-_Hols_Agr__SCEK_PK0000098_64_20260726-BODS_V1_1.xml', 'VJ956', 'SCEK:PK0000098:64:502', '2400A039200A_2400A039210A', '11:32:00', '2400A039200A', '2400A039210A', 0.3056669426758975, 1.0, 1.1772857783344257, -0.17728577833442571, 0.17728577833442571, 0, 'Within expected range');
INSERT INTO predictions (prediction_id, model_name, source_file, vehicle_journey_code, line_ref, segment_key, departure_time, from_stop_ref, to_stop_ref, distance_km, actual_runtime_minutes, predicted_runtime_minutes, residual_minutes, absolute_error_minutes, is_model_anomaly, anomaly_direction) VALUES (2, 'Gradient-Boosted Trees', '323-323--SCEK-SVSE-2026-04-12-Hastings_-_12th_April_2026_-_Copy_o__SCEK_PK0000098_281_20260726-BODS_V1_1.xml', 'VJ1301', 'SCEK:PK0000098:281:323', '1400HA0030_1400HA0031', '10:00:00', '1400HA0030', '1400HA0031', 0.12530838675040396, 1.0, 1.2560074175265825, -0.2560074175265825, 0.2560074175265825, 0, 'Within expected range');
INSERT INTO predictions (prediction_id, model_name, source_file, vehicle_journey_code, line_ref, segment_key, departure_time, from_stop_ref, to_stop_ref, distance_km, actual_runtime_minutes, predicted_runtime_minutes, residual_minutes, absolute_error_minutes, is_model_anomaly, anomaly_direction) VALUES (3, 'Gradient-Boosted Trees', '102-102--SCEK-ASSE-2026-07-26-Ashford_-_26th_July_2026_-_Hols_Agr__SCEK_PK0000098_15_20260726-BODS_V1_1.xml', 'VJ492', 'SCEK:PK0000098:15:103', '2400A027240A_2400A027250A', '08:49:00', '2400A027240A', '2400A027250A', 0.45977427824220357, 2.0, 1.2700118696493374, 0.7299881303506626, 0.7299881303506626, 0, 'Within expected range');
INSERT INTO predictions (prediction_id, model_name, source_file, vehicle_journey_code, line_ref, segment_key, departure_time, from_stop_ref, to_stop_ref, distance_km, actual_runtime_minutes, predicted_runtime_minutes, residual_minutes, absolute_error_minutes, is_model_anomaly, anomaly_direction) VALUES (4, 'Gradient-Boosted Trees', 'LOOP-None--SCEK-THSE-2026-06-07-7_June_2026_with_OTv2__SCEK_PK0000098_146_20260726-BODS_V1_1.xml', 'VJ529', 'SCEK:PK0000098:146:LOOP', '2400100384_2400A046740A', '07:40:00', '2400100384', '2400A046740A', 0.14245494305254064, 1.0, 1.0323938424392196, -0.03239384243921961, 0.03239384243921961, 0, 'Within expected range');
INSERT INTO predictions (prediction_id, model_name, source_file, vehicle_journey_code, line_ref, segment_key, departure_time, from_stop_ref, to_stop_ref, distance_km, actual_runtime_minutes, predicted_runtime_minutes, residual_minutes, absolute_error_minutes, is_model_anomaly, anomaly_direction) VALUES (5, 'Gradient-Boosted Trees', '501A-501--SCEK-ASSE-2026-07-26-Ashford_-_26th_July_2026_-_Hols_Ag__SCEK_PK0000098_363_20260726-BODS_V1_1.xml', 'VJ3', 'SCEK:PK0000098:363:501E', '2400A026230A_240099778', '07:04:00', '2400A026230A', '240099778', 0.3598319087396061, 1.0, 1.301293917656254, -0.3012939176562539, 0.3012939176562539, 0, 'Within expected range');
COMMIT;

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

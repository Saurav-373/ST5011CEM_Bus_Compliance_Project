from __future__ import annotations

import csv
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable, Sequence

import pyarrow.parquet as pq


STOP_FILE = Path("data/interim/naptan_stagecoach_stops.csv")
SEGMENT_FILE = Path("data/processed/segments_enriched.parquet")
MODEL_RESULTS_FILE = Path("outputs/metrics/model_comparison.csv")
BEST_MODEL_FILE = Path("outputs/metrics/best_model_summary.csv")
PREDICTION_FILE = Path("outputs/predictions/test_predictions.parquet")

DATABASE_FILE = Path("database/bus_analytics.db")
SCHEMA_EXPORT_FILE = Path("database/schema.sql")
SAMPLE_EXPORT_FILE = Path("database/sample_export.sql")
DATABASE_DIAGRAM_FILE = Path("database/database_schema.md")
PARAMETERISED_QUERY_FILE = Path("sql/parameterised_queries.sql")

SUMMARY_FILE = Path("outputs/metrics/database_summary.csv")
ROUTE_QUERY_RESULT_FILE = Path("outputs/metrics/database_route_query.csv")
ANOMALY_QUERY_RESULT_FILE = Path("outputs/metrics/database_anomaly_query.csv")
SECURITY_TEST_FILE = Path("outputs/metrics/database_security_test.csv")

BATCH_SIZE = 10_000


TABLE_SCHEMA_SQL = """
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
""".strip()


INDEX_SQL = """
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
""".strip()


PARAMETERISED_QUERY_SQL = """
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
""".strip()


DATABASE_DIAGRAM = """
# Database Schema

```mermaid
erDiagram
    STOPS ||--o{ SEGMENTS : "origin stop"
    STOPS ||--o{ SEGMENTS : "destination stop"
    STOPS ||--o{ PREDICTIONS : "origin stop"
    STOPS ||--o{ PREDICTIONS : "destination stop"
    MODEL_RESULTS ||--o{ PREDICTIONS : "produces"

    STOPS {
        TEXT stop_ref PK
        TEXT stop_name
        TEXT locality_name
        TEXT town
        REAL latitude
        REAL longitude
        TEXT stop_type
        TEXT status
        TEXT administrative_area_code
        TEXT atco_area_code
    }

    SEGMENTS {
        INTEGER segment_id PK
        TEXT source_file
        TEXT vehicle_journey_code
        TEXT vehicle_timing_link_id
        TEXT line_ref
        TEXT segment_key
        TEXT departure_time
        INTEGER departure_hour
        TEXT time_of_day
        INTEGER is_peak_hour
        TEXT from_stop_ref FK
        TEXT to_stop_ref FK
        REAL distance_km
        REAL runtime_minutes
        INTEGER is_iqr_high_duration
    }

    MODEL_RESULTS {
        TEXT model_name PK
        INTEGER rank_by_rmse
        TEXT configuration
        REAL rmse
        REAL mae
        REAL r2
        REAL training_seconds
        REAL evaluation_seconds
    }

    PREDICTIONS {
        INTEGER prediction_id PK
        TEXT model_name FK
        TEXT source_file
        TEXT vehicle_journey_code
        TEXT line_ref
        TEXT segment_key
        TEXT departure_time
        TEXT from_stop_ref FK
        TEXT to_stop_ref FK
        REAL distance_km
        REAL actual_runtime_minutes
        REAL predicted_runtime_minutes
        REAL residual_minutes
        REAL absolute_error_minutes
        INTEGER is_model_anomaly
        TEXT anomaly_direction
    }
```
""".strip()


SEGMENT_COLUMNS = [
    "source_file",
    "vehicle_journey_code",
    "vehicle_timing_link_id",
    "line_ref",
    "segment_key",
    "departure_time",
    "departure_hour",
    "time_of_day",
    "is_peak_hour",
    "from_stop_ref",
    "to_stop_ref",
    "distance_km",
    "runtime_minutes",
    "is_iqr_high_duration",
]


PREDICTION_COLUMNS = [
    "source_file",
    "vehicle_journey_code",
    "line_ref",
    "segment_key",
    "departure_time",
    "from_stop_ref",
    "to_stop_ref",
    "distance_km",
    "runtime_minutes",
    "prediction",
    "residual_minutes",
    "absolute_error_minutes",
    "is_model_anomaly",
    "anomaly_direction",
]


def ensure_output_directories() -> None:
    """Create folders used by this stage."""

    for directory in {
        DATABASE_FILE.parent,
        PARAMETERISED_QUERY_FILE.parent,
        SUMMARY_FILE.parent,
    }:
        directory.mkdir(parents=True, exist_ok=True)


def check_required_files() -> None:
    """Stop immediately when an expected input is missing."""

    required_files = [
        STOP_FILE,
        SEGMENT_FILE,
        MODEL_RESULTS_FILE,
        BEST_MODEL_FILE,
        PREDICTION_FILE,
    ]

    missing_files = [
        file_path
        for file_path in required_files
        if not file_path.exists()
    ]

    if missing_files:
        formatted = "\n".join(
            f"- {file_path}"
            for file_path in missing_files
        )

        raise FileNotFoundError(
            "Required database inputs are missing:\n"
            f"{formatted}"
        )


def validate_parquet_columns(
    parquet_file: Path,
    required_columns: Sequence[str],
) -> None:
    """Confirm that a Parquet file has the expected fields."""

    available_columns = set(
        pq.read_schema(parquet_file).names
    )

    missing_columns = sorted(
        set(required_columns) - available_columns
    )

    if missing_columns:
        raise ValueError(
            f"{parquet_file} is missing columns: "
            + ", ".join(missing_columns)
        )


def save_metric_rows(
    output_file: Path,
    rows: Iterable[tuple[str, Any]],
) -> None:
    """Write metric/value rows to CSV."""

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with output_file.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as output:
        writer = csv.writer(output)
        writer.writerow(["metric", "value"])
        writer.writerows(rows)


def sql_literal(value: Any) -> str:
    """Convert a value to a safe SQL export literal."""

    if value is None:
        return "NULL"

    if isinstance(value, bool):
        return "1" if value else "0"

    if isinstance(value, (int, float)):
        return str(value)

    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def create_database() -> sqlite3.Connection:
    """Create a fresh database containing the four tables."""

    if DATABASE_FILE.exists():
        DATABASE_FILE.unlink()

    connection = sqlite3.connect(DATABASE_FILE)

    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = DELETE")
    connection.execute("PRAGMA synchronous = NORMAL")
    connection.execute("PRAGMA temp_store = MEMORY")
    connection.execute("PRAGMA cache_size = -64000")
    connection.executescript(TABLE_SCHEMA_SQL)

    return connection


def import_stops(
    connection: sqlite3.Connection,
) -> int:
    """Import the matched NaPTAN stop catalogue."""

    insert_sql = """
        INSERT INTO stops (
            stop_ref,
            stop_name,
            locality_name,
            town,
            latitude,
            longitude,
            stop_type,
            status,
            administrative_area_code,
            atco_area_code
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    rows: list[tuple[Any, ...]] = []

    with STOP_FILE.open(
        "r",
        newline="",
        encoding="utf-8-sig",
    ) as input_file:
        reader = csv.DictReader(input_file)

        required_headers = {
            "stop_ref",
            "latitude",
            "longitude",
        }

        missing_headers = required_headers - set(
            reader.fieldnames or []
        )

        if missing_headers:
            raise ValueError(
                f"{STOP_FILE} is missing headers: "
                + ", ".join(sorted(missing_headers))
            )

        for row in reader:
            stop_ref = row["stop_ref"].strip()

            if not stop_ref:
                continue

            stop_name = (
                row.get("naptan_common_name", "").strip()
                or row.get(
                    "timetable_common_name",
                    "",
                ).strip()
                or "Unknown stop"
            )

            rows.append(
                (
                    stop_ref,
                    stop_name,
                    row.get("locality_name", "").strip(),
                    row.get("town", "").strip(),
                    float(row["latitude"]),
                    float(row["longitude"]),
                    row.get("stop_type", "").strip(),
                    row.get("status", "").strip(),
                    row.get(
                        "administrative_area_code",
                        "",
                    ).strip(),
                    row.get("atco_area_code", "").strip(),
                )
            )

    with connection:
        connection.executemany(insert_sql, rows)

    return len(rows)


def import_segments(
    connection: sqlite3.Connection,
) -> int:
    """Stream enriched segment rows into SQLite."""

    validate_parquet_columns(
        SEGMENT_FILE,
        SEGMENT_COLUMNS,
    )

    insert_sql = """
        INSERT INTO segments (
            source_file,
            vehicle_journey_code,
            vehicle_timing_link_id,
            line_ref,
            segment_key,
            departure_time,
            departure_hour,
            time_of_day,
            is_peak_hour,
            from_stop_ref,
            to_stop_ref,
            distance_km,
            runtime_minutes,
            is_iqr_high_duration
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    parquet_file = pq.ParquetFile(SEGMENT_FILE)
    inserted_rows = 0

    connection.execute("BEGIN")

    try:
        for batch in parquet_file.iter_batches(
            batch_size=BATCH_SIZE,
            columns=SEGMENT_COLUMNS,
        ):
            values = [
                (
                    row["source_file"],
                    row["vehicle_journey_code"],
                    row["vehicle_timing_link_id"],
                    row["line_ref"],
                    row["segment_key"],
                    row["departure_time"],
                    int(row["departure_hour"]),
                    row["time_of_day"],
                    int(row["is_peak_hour"]),
                    row["from_stop_ref"],
                    row["to_stop_ref"],
                    float(row["distance_km"]),
                    float(row["runtime_minutes"]),
                    int(row["is_iqr_high_duration"]),
                )
                for row in batch.to_pylist()
            ]

            connection.executemany(
                insert_sql,
                values,
            )

            inserted_rows += len(values)

            if (
                inserted_rows % 50_000 == 0
                or inserted_rows
                == parquet_file.metadata.num_rows
            ):
                print(
                    "Segment rows imported: "
                    f"{inserted_rows:,}"
                )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    return inserted_rows


def import_model_results(
    connection: sqlite3.Connection,
) -> int:
    """Import the model-comparison table."""

    insert_sql = """
        INSERT INTO model_results (
            model_name,
            rank_by_rmse,
            configuration,
            rmse,
            mae,
            r2,
            training_seconds,
            evaluation_seconds
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    rows: list[tuple[Any, ...]] = []

    with MODEL_RESULTS_FILE.open(
        "r",
        newline="",
        encoding="utf-8-sig",
    ) as input_file:
        reader = csv.DictReader(input_file)

        for row in reader:
            rows.append(
                (
                    row["model_name"],
                    int(row["rank_by_rmse"]),
                    row["configuration"],
                    float(row["rmse"]),
                    float(row["mae"]),
                    float(row["r2"]),
                    float(row["training_seconds"]),
                    float(row["evaluation_seconds"]),
                )
            )

    with connection:
        connection.executemany(insert_sql, rows)

    return len(rows)


def read_best_model_name() -> str:
    """Read the chosen model from the metric/value CSV."""

    with BEST_MODEL_FILE.open(
        "r",
        newline="",
        encoding="utf-8-sig",
    ) as input_file:
        reader = csv.DictReader(input_file)

        values = {
            row["metric"]: row["value"]
            for row in reader
        }

    best_model_name = values.get(
        "best_model_name",
        "",
    ).strip()

    if not best_model_name:
        raise ValueError(
            "best_model_name is missing from "
            f"{BEST_MODEL_FILE}"
        )

    return best_model_name


def import_predictions(
    connection: sqlite3.Connection,
    best_model_name: str,
) -> int:
    """Stream best-model test predictions into SQLite."""

    validate_parquet_columns(
        PREDICTION_FILE,
        PREDICTION_COLUMNS,
    )

    model_exists = connection.execute(
        """
        SELECT COUNT(*)
        FROM model_results
        WHERE model_name = ?
        """,
        (best_model_name,),
    ).fetchone()[0]

    if model_exists != 1:
        raise ValueError(
            "The best model is not present in model_results: "
            f"{best_model_name}"
        )

    insert_sql = """
        INSERT INTO predictions (
            model_name,
            source_file,
            vehicle_journey_code,
            line_ref,
            segment_key,
            departure_time,
            from_stop_ref,
            to_stop_ref,
            distance_km,
            actual_runtime_minutes,
            predicted_runtime_minutes,
            residual_minutes,
            absolute_error_minutes,
            is_model_anomaly,
            anomaly_direction
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    parquet_file = pq.ParquetFile(PREDICTION_FILE)
    inserted_rows = 0

    connection.execute("BEGIN")

    try:
        for batch in parquet_file.iter_batches(
            batch_size=BATCH_SIZE,
            columns=PREDICTION_COLUMNS,
        ):
            values = [
                (
                    best_model_name,
                    row["source_file"],
                    row["vehicle_journey_code"],
                    row["line_ref"],
                    row["segment_key"],
                    row["departure_time"],
                    row["from_stop_ref"],
                    row["to_stop_ref"],
                    float(row["distance_km"]),
                    float(row["runtime_minutes"]),
                    float(row["prediction"]),
                    float(row["residual_minutes"]),
                    float(row["absolute_error_minutes"]),
                    int(row["is_model_anomaly"]),
                    row["anomaly_direction"],
                )
                for row in batch.to_pylist()
            ]

            connection.executemany(
                insert_sql,
                values,
            )

            inserted_rows += len(values)

            if (
                inserted_rows % 20_000 == 0
                or inserted_rows
                == parquet_file.metadata.num_rows
            ):
                print(
                    "Prediction rows imported: "
                    f"{inserted_rows:,}"
                )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    return inserted_rows


def create_indexes(
    connection: sqlite3.Connection,
) -> int:
    """Create indexes after the bulk imports."""

    connection.executescript(INDEX_SQL)

    return int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM sqlite_master
            WHERE type = 'index'
              AND name NOT LIKE 'sqlite_autoindex_%'
            """
        ).fetchone()[0]
    )


def run_parameterised_queries(
    connection: sqlite3.Connection,
) -> tuple[int, int, bool]:
    """Run safe query demonstrations and save their results."""

    top_route_row = connection.execute(
        """
        SELECT line_ref
        FROM segments
        GROUP BY line_ref
        ORDER BY COUNT(*) DESC
        LIMIT 1
        """
    ).fetchone()

    if top_route_row is None:
        raise RuntimeError(
            "The database contains no service lines."
        )

    selected_route = str(top_route_row[0])

    route_result = connection.execute(
        """
        SELECT
            line_ref,
            COUNT(*) AS segment_records,
            ROUND(AVG(runtime_minutes), 4)
                AS average_runtime_minutes,
            ROUND(AVG(distance_km), 4)
                AS average_distance_km,
            SUM(is_iqr_high_duration)
                AS high_duration_records
        FROM segments
        WHERE line_ref = ?
        GROUP BY line_ref
        """,
        (selected_route,),
    ).fetchone()

    ROUTE_QUERY_RESULT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with ROUTE_QUERY_RESULT_FILE.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as output:
        writer = csv.writer(output)
        writer.writerow(
            [
                "line_ref",
                "segment_records",
                "average_runtime_minutes",
                "average_distance_km",
                "high_duration_records",
            ]
        )

        if route_result is not None:
            writer.writerow(route_result)

    anomaly_results = connection.execute(
        """
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
        LIMIT ?
        """,
        (1, 2.0, 10),
    ).fetchall()

    with ANOMALY_QUERY_RESULT_FILE.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as output:
        writer = csv.writer(output)
        writer.writerow(
            [
                "line_ref",
                "from_stop_ref",
                "to_stop_ref",
                "actual_runtime_minutes",
                "predicted_runtime_minutes",
                "absolute_error_minutes",
                "anomaly_direction",
            ]
        )
        writer.writerows(anomaly_results)

    malicious_input = "' OR 1=1 --"

    malicious_result_count = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM segments
            WHERE line_ref = ?
            """,
            (malicious_input,),
        ).fetchone()[0]
    )

    total_segment_count = int(
        connection.execute(
            "SELECT COUNT(*) FROM segments"
        ).fetchone()[0]
    )

    injection_prevented = (
        malicious_result_count == 0
        and total_segment_count > 0
    )

    save_metric_rows(
        SECURITY_TEST_FILE,
        [
            (
                "query_method",
                "parameterised question-mark placeholder",
            ),
            (
                "malicious_test_input",
                malicious_input,
            ),
            (
                "rows_returned_for_malicious_input",
                malicious_result_count,
            ),
            (
                "total_segment_records_unchanged",
                total_segment_count,
            ),
            (
                "sql_injection_prevented",
                injection_prevented,
            ),
        ],
    )

    return (
        len(anomaly_results),
        malicious_result_count,
        injection_prevented,
    )


def write_schema_exports() -> None:
    """Write schema, query examples, and Mermaid diagram."""

    SCHEMA_EXPORT_FILE.write_text(
        TABLE_SCHEMA_SQL
        + "\n\n"
        + INDEX_SQL
        + "\n",
        encoding="utf-8",
    )

    PARAMETERISED_QUERY_FILE.write_text(
        PARAMETERISED_QUERY_SQL + "\n",
        encoding="utf-8",
    )

    DATABASE_DIAGRAM_FILE.write_text(
        DATABASE_DIAGRAM + "\n",
        encoding="utf-8",
    )


def write_sample_export(
    connection: sqlite3.Connection,
) -> None:
    """Create a small SQL export containing five rows per table."""

    schema_without_pragma = TABLE_SCHEMA_SQL.replace(
        "PRAGMA foreign_keys = ON;\n\n",
        "",
        1,
    )

    lines = [
        "-- ST5011CEM sample SQLite export",
        "-- Run against a new empty SQLite database.",
        "",
        "PRAGMA foreign_keys = ON;",
        "",
        schema_without_pragma,
        "",
    ]

    for table_name in [
        "stops",
        "model_results",
        "segments",
        "predictions",
    ]:
        column_rows = connection.execute(
            f"PRAGMA table_info({table_name})"
        ).fetchall()

        column_names = [
            str(row[1])
            for row in column_rows
        ]

        sample_rows = connection.execute(
            f"SELECT * FROM {table_name} LIMIT 5"
        ).fetchall()

        lines.append("BEGIN TRANSACTION;")
        lines.append(
            f"-- Five sample rows from {table_name}"
        )

        for row in sample_rows:
            columns_text = ", ".join(column_names)

            values_text = ", ".join(
                sql_literal(value)
                for value in row
            )

            lines.append(
                f"INSERT INTO {table_name} "
                f"({columns_text}) "
                f"VALUES ({values_text});"
            )

        lines.append("COMMIT;")
        lines.append("")

    lines.append(INDEX_SQL)
    lines.append("")

    SAMPLE_EXPORT_FILE.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )


def save_database_summary(
    connection: sqlite3.Connection,
    build_seconds: float,
    anomaly_query_rows: int,
    malicious_result_count: int,
    injection_prevented: bool,
    custom_index_count: int,
) -> None:
    """Save counts, integrity checks, and security results."""

    table_counts = {
        table_name: int(
            connection.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]
        )
        for table_name in [
            "stops",
            "segments",
            "model_results",
            "predictions",
        ]
    }

    foreign_key_violations = (
        connection.execute(
            "PRAGMA foreign_key_check"
        ).fetchall()
    )

    integrity_result = str(
        connection.execute(
            "PRAGMA integrity_check"
        ).fetchone()[0]
    )

    database_size_bytes = (
        DATABASE_FILE.stat().st_size
    )

    save_metric_rows(
        SUMMARY_FILE,
        [
            ("database_file", str(DATABASE_FILE)),
            (
                "database_size_bytes",
                database_size_bytes,
            ),
            (
                "database_size_megabytes",
                round(
                    database_size_bytes
                    / (1024 * 1024),
                    4,
                ),
            ),
            (
                "stop_records",
                table_counts["stops"],
            ),
            (
                "segment_records",
                table_counts["segments"],
            ),
            (
                "model_result_records",
                table_counts["model_results"],
            ),
            (
                "prediction_records",
                table_counts["predictions"],
            ),
            (
                "foreign_key_violations",
                len(foreign_key_violations),
            ),
            ("integrity_check", integrity_result),
            ("custom_indexes", custom_index_count),
            ("parameterised_route_query", True),
            ("parameterised_anomaly_query", True),
            (
                "anomaly_query_rows_returned",
                anomaly_query_rows,
            ),
            (
                "malicious_input_rows_returned",
                malicious_result_count,
            ),
            (
                "sql_injection_prevented",
                injection_prevented,
            ),
            (
                "database_build_seconds",
                round(build_seconds, 4),
            ),
        ],
    )


def main() -> None:
    print("\n" + "=" * 72)
    print("SQLITE DATABASE DESIGN AND IMPLEMENTATION")
    print("=" * 72)

    ensure_output_directories()
    check_required_files()
    write_schema_exports()

    build_start = time.perf_counter()
    connection = create_database()

    try:
        print("\nImporting NaPTAN stops...")
        stop_count = import_stops(connection)
        print(f"Stops imported: {stop_count:,}")

        print("\nImporting enriched segments...")
        segment_count = import_segments(connection)
        print(f"Segments imported: {segment_count:,}")

        print("\nImporting model comparison results...")
        model_count = import_model_results(connection)
        print(
            f"Model results imported: {model_count:,}"
        )

        best_model_name = read_best_model_name()
        print(f"Best model: {best_model_name}")

        print("\nImporting test predictions...")
        prediction_count = import_predictions(
            connection,
            best_model_name,
        )
        print(
            f"Predictions imported: "
            f"{prediction_count:,}"
        )

        print("\nCreating database indexes...")
        custom_index_count = create_indexes(connection)
        print(
            f"Custom indexes created: "
            f"{custom_index_count:,}"
        )

        print(
            "\nRunning secure parameterised queries..."
        )

        (
            anomaly_query_rows,
            malicious_result_count,
            injection_prevented,
        ) = run_parameterised_queries(connection)

        connection.execute("ANALYZE")
        connection.commit()

        write_sample_export(connection)

        build_seconds = (
            time.perf_counter() - build_start
        )

        save_database_summary(
            connection=connection,
            build_seconds=build_seconds,
            anomaly_query_rows=anomaly_query_rows,
            malicious_result_count=(
                malicious_result_count
            ),
            injection_prevented=injection_prevented,
            custom_index_count=custom_index_count,
        )

        foreign_key_violations = (
            connection.execute(
                "PRAGMA foreign_key_check"
            ).fetchall()
        )

        integrity_check = (
            connection.execute(
                "PRAGMA integrity_check"
            ).fetchone()[0]
        )

        print("\nDatabase verification:")
        print(f"Stop records: {stop_count:,}")
        print(f"Segment records: {segment_count:,}")
        print(
            f"Model result records: "
            f"{model_count:,}"
        )
        print(
            f"Prediction records: "
            f"{prediction_count:,}"
        )
        print(
            "Foreign-key violations: "
            f"{len(foreign_key_violations):,}"
        )
        print(f"Integrity check: {integrity_check}")
        print(
            "Malicious-input rows returned: "
            f"{malicious_result_count:,}"
        )
        print(
            "SQL injection prevented: "
            f"{injection_prevented}"
        )
        print(
            f"Build time: "
            f"{build_seconds:.2f} seconds"
        )

        print("\nOutputs:")
        print(f"SQLite database: {DATABASE_FILE}")
        print(f"Schema export: {SCHEMA_EXPORT_FILE}")
        print(
            f"Sample SQL export: "
            f"{SAMPLE_EXPORT_FILE}"
        )
        print(
            f"Schema diagram: "
            f"{DATABASE_DIAGRAM_FILE}"
        )
        print(
            "Parameterised queries: "
            f"{PARAMETERISED_QUERY_FILE}"
        )
        print(
            f"Database summary: "
            f"{SUMMARY_FILE}"
        )
        print("=" * 72)
        print(
            "Database stage completed successfully."
        )

    except Exception as error:
        print("\nDatabase stage failed.")
        print(
            f"Error type: "
            f"{type(error).__name__}"
        )
        print(
            f"Error message: "
            f"{error}"
        )
        raise

    finally:
        connection.close()
        print("SQLite connection closed safely.")


if __name__ == "__main__":
    main()
from __future__ import annotations

import csv
import sqlite3
import unittest
from pathlib import Path

import pyarrow.parquet as pq


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATABASE_FILE = (
    PROJECT_ROOT / "database" / "bus_analytics.db"
)

CLEANED_PARQUET = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "segments_cleaned.parquet"
)

ENRICHED_PARQUET = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "segments_enriched.parquet"
)

PREDICTION_PARQUET = (
    PROJECT_ROOT
    / "outputs"
    / "predictions"
    / "test_predictions.parquet"
)

CLEANING_SUMMARY = (
    PROJECT_ROOT
    / "outputs"
    / "metrics"
    / "data_cleaning_summary.csv"
)

FEATURE_SUMMARY = (
    PROJECT_ROOT
    / "outputs"
    / "metrics"
    / "feature_engineering_summary.csv"
)

MODEL_COMPARISON = (
    PROJECT_ROOT
    / "outputs"
    / "metrics"
    / "model_comparison.csv"
)

MODEL_SPLIT_SUMMARY = (
    PROJECT_ROOT
    / "outputs"
    / "metrics"
    / "model_split_summary.csv"
)

BEST_MODEL_SUMMARY = (
    PROJECT_ROOT
    / "outputs"
    / "metrics"
    / "best_model_summary.csv"
)

SECURITY_SUMMARY = (
    PROJECT_ROOT
    / "outputs"
    / "metrics"
    / "database_security_test.csv"
)

DASHBOARD_FILE = (
    PROJECT_ROOT / "src" / "dashboard_app.py"
)


def read_metric_file(
    file_path: Path,
) -> dict[str, str]:
    """Read a metric-value CSV file into a dictionary."""

    with file_path.open(
        "r",
        newline="",
        encoding="utf-8-sig",
    ) as input_file:
        reader = csv.DictReader(input_file)

        return {
            row["metric"]: row["value"]
            for row in reader
        }


def read_csv_rows(
    file_path: Path,
) -> list[dict[str, str]]:
    """Read a normal CSV file into a list of rows."""

    with file_path.open(
        "r",
        newline="",
        encoding="utf-8-sig",
    ) as input_file:
        return list(csv.DictReader(input_file))


def as_boolean(value: str) -> bool:
    """Convert a saved text value into a Boolean."""

    return value.strip().lower() == "true"


class TestBusAnalyticsSystem(unittest.TestCase):
    """Automated validation for the completed analytics system."""

    def test_01_required_files_exist(self) -> None:
        required_files = [
            DATABASE_FILE,
            CLEANED_PARQUET,
            ENRICHED_PARQUET,
            PREDICTION_PARQUET,
            CLEANING_SUMMARY,
            FEATURE_SUMMARY,
            MODEL_COMPARISON,
            MODEL_SPLIT_SUMMARY,
            BEST_MODEL_SUMMARY,
            SECURITY_SUMMARY,
            DASHBOARD_FILE,
        ]

        missing_files = [
            str(file_path)
            for file_path in required_files
            if not file_path.exists()
        ]

        self.assertEqual(
            missing_files,
            [],
            msg=(
                "Required project files are missing:\n"
                + "\n".join(missing_files)
            ),
        )

    def test_02_cleaned_dataset_counts(self) -> None:
        metrics = read_metric_file(
            CLEANING_SUMMARY
        )

        self.assertEqual(
            int(metrics["raw_records"]),
            445_788,
        )

        self.assertEqual(
            int(metrics["final_cleaned_records"]),
            375_668,
        )

        self.assertEqual(
            int(metrics["parquet_rows_written"]),
            375_668,
        )

        partition_value = metrics.get(
            "final_spark_partitions",
            metrics.get("output_partitions", "0"),
        )

        self.assertGreaterEqual(
            int(partition_value),
            4,
        )

        parquet_rows = (
            pq.ParquetFile(
                CLEANED_PARQUET
            ).metadata.num_rows
        )

        self.assertEqual(
            parquet_rows,
            375_668,
        )

    def test_03_feature_engineering_results(self) -> None:
        metrics = read_metric_file(
            FEATURE_SUMMARY
        )

        self.assertEqual(
            int(metrics["enriched_segment_records"]),
            375_668,
        )

        self.assertEqual(
            int(metrics["origin_unmatched_records"]),
            0,
        )

        self.assertEqual(
            int(
                metrics[
                    "destination_unmatched_records"
                ]
            ),
            0,
        )

        self.assertEqual(
            float(
                metrics[
                    "both_stops_match_percentage"
                ]
            ),
            100.0,
        )

        self.assertTrue(
            as_boolean(
                metrics[
                    "broadcast_join_detected"
                ]
            )
        )

        self.assertEqual(
            int(metrics["missing_distance_records"]),
            0,
        )

        parquet_rows = (
            pq.ParquetFile(
                ENRICHED_PARQUET
            ).metadata.num_rows
        )

        self.assertEqual(
            parquet_rows,
            375_668,
        )

    def test_04_leakage_safe_split(self) -> None:
        metrics = read_metric_file(
            MODEL_SPLIT_SUMMARY
        )

        training_records = int(
            metrics["training_records"]
        )

        testing_records = int(
            metrics["testing_records"]
        )

        self.assertEqual(
            training_records + testing_records,
            375_668,
        )

        self.assertEqual(
            int(metrics["overlapping_segments"]),
            0,
        )

        self.assertGreaterEqual(
            int(
                metrics[
                    "training_spark_partitions"
                ]
            ),
            4,
        )

        self.assertGreaterEqual(
            int(
                metrics[
                    "testing_spark_partitions"
                ]
            ),
            4,
        )

    def test_05_model_comparison(self) -> None:
        model_rows = read_csv_rows(
            MODEL_COMPARISON
        )

        self.assertGreaterEqual(
            len(model_rows),
            3,
        )

        for row in model_rows:
            self.assertGreaterEqual(
                float(row["rmse"]),
                0.0,
            )

            self.assertGreaterEqual(
                float(row["mae"]),
                0.0,
            )

            self.assertLessEqual(
                float(row["r2"]),
                1.0,
            )

            self.assertGreater(
                float(row["training_seconds"]),
                0.0,
            )

        ranked_best = min(
            model_rows,
            key=lambda row: int(
                row["rank_by_rmse"]
            ),
        )

        best_summary = read_metric_file(
            BEST_MODEL_SUMMARY
        )

        self.assertEqual(
            ranked_best["model_name"],
            best_summary["best_model_name"],
        )

        self.assertAlmostEqual(
            float(ranked_best["rmse"]),
            float(
                best_summary[
                    "best_model_rmse"
                ]
            ),
            places=6,
        )

    def test_06_prediction_output(self) -> None:
        parquet_file = pq.ParquetFile(
            PREDICTION_PARQUET
        )

        self.assertEqual(
            parquet_file.metadata.num_rows,
            72_832,
        )

        required_columns = {
            "line_ref",
            "segment_key",
            "runtime_minutes",
            "prediction",
            "residual_minutes",
            "absolute_error_minutes",
            "is_model_anomaly",
            "anomaly_direction",
        }

        available_columns = set(
            parquet_file.schema.names
        )

        self.assertTrue(
            required_columns.issubset(
                available_columns
            )
        )

    def test_07_database_integrity(self) -> None:
        connection = sqlite3.connect(
            DATABASE_FILE
        )

        try:
            connection.execute(
                "PRAGMA foreign_keys = ON"
            )

            integrity_result = (
                connection.execute(
                    "PRAGMA integrity_check"
                ).fetchone()[0]
            )

            foreign_key_violations = (
                connection.execute(
                    "PRAGMA foreign_key_check"
                ).fetchall()
            )

            table_counts = {
                "stops": 5_143,
                "segments": 375_668,
                "model_results": 4,
                "predictions": 72_832,
            }

            self.assertEqual(
                integrity_result,
                "ok",
            )

            self.assertEqual(
                foreign_key_violations,
                [],
            )

            for (
                table_name,
                expected_count,
            ) in table_counts.items():
                actual_count = int(
                    connection.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM {table_name}
                        """
                    ).fetchone()[0]
                )

                self.assertEqual(
                    actual_count,
                    expected_count,
                )

            invalid_segment_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM segments
                    WHERE runtime_minutes <= 0
                       OR distance_km <= 0
                       OR from_stop_ref IS NULL
                       OR to_stop_ref IS NULL
                    """
                ).fetchone()[0]
            )

            self.assertEqual(
                invalid_segment_count,
                0,
            )

        finally:
            connection.close()

    def test_08_parameterised_query_security(self) -> None:
        malicious_input = "' OR 1=1 --"

        connection = sqlite3.connect(
            DATABASE_FILE
        )

        try:
            result_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM segments
                    WHERE line_ref = ?
                    """,
                    (malicious_input,),
                ).fetchone()[0]
            )

            total_records = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM segments
                    """
                ).fetchone()[0]
            )

            self.assertEqual(
                result_count,
                0,
            )

            self.assertEqual(
                total_records,
                375_668,
            )

        finally:
            connection.close()

        saved_security = read_metric_file(
            SECURITY_SUMMARY
        )

        self.assertTrue(
            as_boolean(
                saved_security[
                    "sql_injection_prevented"
                ]
            )
        )

    def test_09_dashboard_source_compiles(self) -> None:
        source_text = DASHBOARD_FILE.read_text(
            encoding="utf-8"
        )

        compile(
            source_text,
            str(DASHBOARD_FILE),
            "exec",
        )

        self.assertIn(
            "PRAGMA query_only = ON",
            source_text,
        )

        self.assertIn(
            "WHERE line_ref = ?",
            source_text,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
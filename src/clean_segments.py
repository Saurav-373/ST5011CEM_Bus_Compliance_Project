from __future__ import annotations

import csv
import shutil
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingest_and_profile_segments import build_schema


INPUT_FILE = Path(
    "data/interim/stagecoach_southeast_segments.csv"
)

OUTPUT_PARQUET = Path(
    "data/processed/segments_cleaned.parquet"
)

# Folder left behind by the earlier failed Spark write.
OLD_OUTPUT_DIRECTORY = Path(
    "data/processed/segments_cleaned_parquet"
)

SUMMARY_FILE = Path(
    "outputs/metrics/data_cleaning_summary.csv"
)


def create_spark_session() -> SparkSession:
    """Create the Spark session used for data cleaning."""

    return (
        SparkSession.builder
        .appName("ST5011CEM_Timetable_Data_Cleaning")
        .master("local[*]")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def write_parquet_with_pyarrow(
    dataframe: DataFrame,
    output_path: Path,
    batch_size: int = 25_000,
) -> int:
    """
    Stream cleaned Spark rows into one Parquet file.

    PyArrow is used only for final persistence because Hadoop's
    native Windows output committer is unavailable on this machine.
    All cleaning and transformations still take place in PySpark.
    """

    import pyarrow as pa
    import pyarrow.parquet as pq

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if output_path.exists():
        output_path.unlink()

    parquet_writer = None
    batch: list[dict] = []
    rows_written = 0

    try:
        for row in dataframe.toLocalIterator():
            batch.append(row.asDict(recursive=True))

            if len(batch) >= batch_size:
                table = pa.Table.from_pylist(batch)

                if parquet_writer is None:
                    parquet_writer = pq.ParquetWriter(
                        str(output_path),
                        table.schema,
                        compression="snappy",
                    )

                parquet_writer.write_table(table)
                rows_written += len(batch)
                batch.clear()

        # Write the final incomplete batch.
        if batch:
            table = pa.Table.from_pylist(batch)

            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(
                    str(output_path),
                    table.schema,
                    compression="snappy",
                )

            parquet_writer.write_table(table)
            rows_written += len(batch)
            batch.clear()

    finally:
        if parquet_writer is not None:
            parquet_writer.close()

    if rows_written == 0:
        raise RuntimeError(
            "No records were written to the Parquet file."
        )

    return rows_written


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    cached_cleaned_df: DataFrame | None = None

    try:
        print("\n" + "=" * 65)
        print("PYSPARK DATA CLEANING")
        print("=" * 65)

        if not INPUT_FILE.exists():
            raise FileNotFoundError(
                f"Input dataset was not found: {INPUT_FILE}"
            )

        # Remove incomplete output left by earlier failed attempts.
        if OLD_OUTPUT_DIRECTORY.exists():
            shutil.rmtree(
                OLD_OUTPUT_DIRECTORY,
                ignore_errors=True,
            )

        if OUTPUT_PARQUET.exists():
            OUTPUT_PARQUET.unlink()

        # Load the extracted CSV using the predefined schema.
        raw_df = (
            spark.read
            .option("header", True)
            .option("encoding", "UTF-8")
            .schema(build_schema())
            .csv(str(INPUT_FILE))
            .repartition(4)
        )

        print(
            "Initial partitions: "
            f"{raw_df.rdd.getNumPartitions()}"
        )

        raw_count = raw_df.count()

        # Remove exact duplicate rows.
        deduplicated_df = raw_df.dropDuplicates()
        deduplicated_count = deduplicated_df.count()

        duplicates_removed = (
            raw_count - deduplicated_count
        )

        # Fields required for meaningful segment analysis.
        required_text_columns = [
            "source_file",
            "vehicle_journey_code",
            "line_ref",
            "departure_time",
            "from_stop_ref",
            "to_stop_ref",
        ]

        required_conditions = [
            F.col(column_name).isNotNull()
            & (
                F.trim(
                    F.col(column_name)
                )
                != ""
            )
            for column_name in required_text_columns
        ]

        valid_required_df = deduplicated_df.filter(
            reduce(
                lambda left, right: left & right,
                required_conditions,
            )
        )

        valid_required_count = valid_required_df.count()

        missing_required_removed = (
            deduplicated_count
            - valid_required_count
        )

        # Record data-quality problems before removing them.
        zero_runtime_count = (
            valid_required_df
            .filter(
                F.col("runtime_seconds") == 0
            )
            .count()
        )

        invalid_sequence_count = (
            valid_required_df
            .filter(
                F.col("to_sequence")
                <= F.col("from_sequence")
            )
            .count()
        )

        identical_stop_count = (
            valid_required_df
            .filter(
                F.col("from_stop_ref")
                == F.col("to_stop_ref")
            )
            .count()
        )

        # Clean records and create useful features.
        #
        # The hour is extracted manually instead of using to_timestamp()
        # because some transport timetables can use values beyond 23:59.
        base_cleaned_df = (
            valid_required_df
            .filter(
                F.col("runtime_seconds") > 0
            )
            .filter(
                F.col("to_sequence")
                > F.col("from_sequence")
            )
            .filter(
                F.col("from_stop_ref")
                != F.col("to_stop_ref")
            )
            .withColumn(
                "departure_hour_raw",
                F.regexp_extract(
                    F.col("departure_time"),
                    r"^(\d{1,2}):",
                    1,
                ).cast("integer"),
            )
            .withColumn(
                "departure_hour",
                F.pmod(
                    F.col("departure_hour_raw"),
                    F.lit(24),
                ),
            )
            .drop("departure_hour_raw")
            .withColumn(
                "is_peak_hour",
                F.when(
                    F.col("departure_hour").between(7, 9)
                    | F.col("departure_hour").between(16, 18),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "time_of_day",
                F.when(
                    F.col("departure_hour").between(5, 11),
                    "Morning",
                )
                .when(
                    F.col("departure_hour").between(12, 16),
                    "Afternoon",
                )
                .when(
                    F.col("departure_hour").between(17, 21),
                    "Evening",
                )
                .otherwise("Night"),
            )
            .withColumn(
                "segment_key",
                F.concat_ws(
                    "_",
                    F.col("from_stop_ref"),
                    F.col("to_stop_ref"),
                ),
            )
            .repartition(4)
            .cache()
        )

        cached_cleaned_df = base_cleaned_df

        cleaned_count = cached_cleaned_df.count()
        total_removed = raw_count - cleaned_count

        # Calculate IQR boundaries for investigation.
        # High-duration records are flagged, not automatically deleted.
        q1, q3 = cached_cleaned_df.approxQuantile(
            "runtime_minutes",
            [0.25, 0.75],
            0.01,
        )

        runtime_iqr = q3 - q1
        upper_iqr_limit = q3 + (
            1.5 * runtime_iqr
        )

        final_df = (
            cached_cleaned_df
            .withColumn(
                "is_iqr_high_duration",
                F.when(
                    F.col("runtime_minutes")
                    > F.lit(upper_iqr_limit),
                    1,
                ).otherwise(0),
            )
            .repartition(4)
        )

        high_duration_count = (
            final_df
            .filter(
                F.col("is_iqr_high_duration") == 1
            )
            .count()
        )

        final_partition_count = (
            final_df.rdd.getNumPartitions()
        )

        print("\nCleaning results:")
        print(f"Raw records: {raw_count:,}")
        print(
            f"Duplicates removed: "
            f"{duplicates_removed:,}"
        )
        print(
            "Missing required records removed: "
            f"{missing_required_removed:,}"
        )
        print(
            f"Zero-runtime records identified: "
            f"{zero_runtime_count:,}"
        )
        print(
            f"Invalid sequence records identified: "
            f"{invalid_sequence_count:,}"
        )
        print(
            f"Identical-stop records identified: "
            f"{identical_stop_count:,}"
        )
        print(
            f"Final cleaned records: "
            f"{cleaned_count:,}"
        )
        print(
            f"Total records removed: "
            f"{total_removed:,}"
        )

        print("\nOutlier inspection:")
        print(f"Q1: {q1:.2f} minutes")
        print(f"Q3: {q3:.2f} minutes")
        print(
            f"IQR: {runtime_iqr:.2f} minutes"
        )
        print(
            "IQR upper limit: "
            f"{upper_iqr_limit:.2f} minutes"
        )
        print(
            "High-duration records retained: "
            f"{high_duration_count:,}"
        )
        print(
            "Final Spark partitions: "
            f"{final_partition_count}"
        )

        print("\nWriting cleaned data to Parquet...")

        rows_written = write_parquet_with_pyarrow(
            dataframe=final_df,
            output_path=OUTPUT_PARQUET,
            batch_size=25_000,
        )

        if rows_written != cleaned_count:
            raise RuntimeError(
                "Parquet row count does not match "
                "the cleaned Spark DataFrame count. "
                f"Expected {cleaned_count:,}, "
                f"wrote {rows_written:,}."
            )

        print(
            f"Rows written to Parquet: "
            f"{rows_written:,}"
        )

        # Save a small cleaning summary that can be committed to Git.
        SUMMARY_FILE.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        summary_rows = [
            ("raw_records", raw_count),
            (
                "duplicates_removed",
                duplicates_removed,
            ),
            (
                "missing_required_records_removed",
                missing_required_removed,
            ),
            (
                "zero_runtime_records",
                zero_runtime_count,
            ),
            (
                "invalid_sequence_records",
                invalid_sequence_count,
            ),
            (
                "identical_stop_records",
                identical_stop_count,
            ),
            (
                "final_cleaned_records",
                cleaned_count,
            ),
            (
                "total_records_removed",
                total_removed,
            ),
            (
                "runtime_q1_minutes",
                q1,
            ),
            (
                "runtime_q3_minutes",
                q3,
            ),
            (
                "runtime_iqr_minutes",
                runtime_iqr,
            ),
            (
                "upper_iqr_limit_minutes",
                upper_iqr_limit,
            ),
            (
                "high_duration_records_retained",
                high_duration_count,
            ),
            (
                "final_spark_partitions",
                final_partition_count,
            ),
            (
                "parquet_rows_written",
                rows_written,
            ),
        ]

        with SUMMARY_FILE.open(
            mode="w",
            newline="",
            encoding="utf-8",
        ) as summary_output:
            writer = csv.writer(summary_output)
            writer.writerow(
                ["metric", "value"]
            )
            writer.writerows(summary_rows)

        print("\nOutput files:")
        print(
            f"Cleaned Parquet file: "
            f"{OUTPUT_PARQUET}"
        )
        print(
            f"Cleaning summary: "
            f"{SUMMARY_FILE}"
        )
        print("=" * 65)
        print("Data cleaning completed successfully.")

    except Exception as error:
        print("\nData cleaning failed.")
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
        if cached_cleaned_df is not None:
            cached_cleaned_df.unpersist()

        spark.stop()
        print("SparkSession stopped safely.")


if __name__ == "__main__":
    main()
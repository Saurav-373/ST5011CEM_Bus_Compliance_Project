from pathlib import Path

from pyspark.sql import SparkSession


PARQUET_FILE = Path(
    "data/processed/segments_cleaned.parquet"
)

EXPECTED_ROWS = 375_668
EXPECTED_COLUMNS = 25


def create_spark_session() -> SparkSession:
    """Create a local Spark session for verification."""

    return (
        SparkSession.builder
        .appName("ST5011CEM_Verify_Cleaned_Data")
        .master("local[*]")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        if not PARQUET_FILE.exists():
            raise FileNotFoundError(
                f"Cleaned Parquet file was not found: {PARQUET_FILE}"
            )

        cleaned_df = (
            spark.read
            .parquet(str(PARQUET_FILE))
            .repartition(4)
            .cache()
        )

        row_count = cleaned_df.count()
        column_count = len(cleaned_df.columns)
        partition_count = cleaned_df.rdd.getNumPartitions()

        print("\n" + "=" * 60)
        print("CLEANED PARQUET VERIFICATION")
        print("=" * 60)
        print(f"Rows: {row_count:,}")
        print(f"Columns: {column_count}")
        print(f"Spark partitions: {partition_count}")
        print(f"Cached: {cleaned_df.is_cached}")

        print("\nCleaned dataset schema:")
        cleaned_df.printSchema()

        print("\nSample cleaned records:")
        cleaned_df.select(
            "line_ref",
            "departure_time",
            "departure_hour",
            "is_peak_hour",
            "time_of_day",
            "from_stop_ref",
            "to_stop_ref",
            "runtime_minutes",
            "is_iqr_high_duration",
        ).show(5, truncate=False)

        if row_count != EXPECTED_ROWS:
            raise ValueError(
                f"Expected {EXPECTED_ROWS:,} rows, "
                f"but found {row_count:,}."
            )

        if column_count != EXPECTED_COLUMNS:
            raise ValueError(
                f"Expected {EXPECTED_COLUMNS} columns, "
                f"but found {column_count}."
            )

        if partition_count < 4:
            raise ValueError(
                "The verified DataFrame has fewer than four partitions."
            )

        print("\nAll verification checks passed.")
        print("Cleaned Parquet data is ready for analysis.")
        print("=" * 60)

        cleaned_df.unpersist()

    except Exception as error:
        print("\nVerification failed.")
        print(f"Error type: {type(error).__name__}")
        print(f"Error message: {error}")
        raise

    finally:
        spark.stop()
        print("SparkSession stopped safely.")


if __name__ == "__main__":
    main()
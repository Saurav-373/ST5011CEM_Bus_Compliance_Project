from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


INPUT_FILE = Path(
    "data/interim/stagecoach_southeast_segments.csv"
)


def create_spark_session() -> SparkSession:
    """Create the Spark session used for data profiling."""

    return (
        SparkSession.builder
        .appName("ST5011CEM_Timetable_Data_Profiling")
        .master("local[*]")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def build_schema() -> StructType:
    """Define the expected CSV column types."""

    return StructType(
        [
            StructField("source_file", StringType(), True),
            StructField("vehicle_journey_code", StringType(), True),
            StructField("service_ref", StringType(), True),
            StructField("line_ref", StringType(), True),
            StructField("journey_pattern_ref", StringType(), True),
            StructField("departure_time", StringType(), True),
            StructField("vehicle_timing_link_id", StringType(), True),
            StructField(
                "journey_pattern_timing_link_ref",
                StringType(),
                True,
            ),
            StructField("from_sequence", IntegerType(), True),
            StructField("from_stop_ref", StringType(), True),
            StructField("from_timing_status", StringType(), True),
            StructField("from_activity", StringType(), True),
            StructField("to_sequence", IntegerType(), True),
            StructField("to_stop_ref", StringType(), True),
            StructField("to_timing_status", StringType(), True),
            StructField("to_activity", StringType(), True),
            StructField("route_link_ref", StringType(), True),
            StructField("runtime_iso", StringType(), True),
            StructField("runtime_seconds", IntegerType(), True),
            StructField("runtime_minutes", DoubleType(), True),
        ]
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        if not INPUT_FILE.exists():
            raise FileNotFoundError(
                f"Dataset not found: {INPUT_FILE}"
            )

        print("\n" + "=" * 65)
        print("PYSPARK DATA INGESTION AND PROFILING")
        print("=" * 65)
        print(f"Spark version: {spark.version}")
        print(f"Spark master: {spark.sparkContext.master}")
        print(
            "Shuffle partitions: "
            f"{spark.conf.get('spark.sql.shuffle.partitions')}"
        )
        print(f"Spark UI: {spark.sparkContext.uiWebUrl}")

        # Load the extracted CSV using an explicit schema.
        raw_df = (
            spark.read
            .option("header", True)
            .option("encoding", "UTF-8")
            .schema(build_schema())
            .csv(str(INPUT_FILE))
        )

        print(
            f"\nPartitions immediately after loading: "
            f"{raw_df.rdd.getNumPartitions()}"
        )

        # Ensure the coursework pipeline uses at least four partitions.
        segments_df = raw_df.repartition(4)

        print(
            f"Partitions after repartitioning: "
            f"{segments_df.rdd.getNumPartitions()}"
        )

        # Cache because several profiling actions reuse the same data.
        segments_df.persist(StorageLevel.MEMORY_AND_DISK)

        total_rows = segments_df.count()

        print(f"Total rows: {total_rows:,}")
        print(f"Total columns: {len(segments_df.columns)}")
        print(f"Cached: {segments_df.is_cached}")

        print("\nDataset schema:")
        segments_df.printSchema()

        print("\nFirst five records:")
        segments_df.select(
            "vehicle_journey_code",
            "line_ref",
            "departure_time",
            "from_stop_ref",
            "to_stop_ref",
            "runtime_minutes",
        ).show(5, truncate=False)

        # Count missing values in every column.
        missing_expressions = []

        for column_name in segments_df.columns:
            missing_expressions.append(
                F.sum(
                    F.when(
                        F.col(column_name).isNull()
                        | (
                            F.trim(
                                F.col(column_name).cast("string")
                            )
                            == ""
                        ),
                        1,
                    ).otherwise(0)
                ).alias(column_name)
            )

        print("\nMissing-value counts:")
        segments_df.select(missing_expressions).show(
            truncate=False,
            vertical=True,
        )

        # Count exact duplicate rows.
        distinct_rows = segments_df.dropDuplicates().count()
        duplicate_rows = total_rows - distinct_rows

        print(f"\nDistinct rows: {distinct_rows:,}")
        print(f"Exact duplicate rows: {duplicate_rows:,}")

        positive_runtime_rows = segments_df.filter(
            F.col("runtime_seconds") > 0
        ).count()

        zero_runtime_rows = segments_df.filter(
            F.col("runtime_seconds") == 0
        ).count()

        missing_runtime_rows = segments_df.filter(
            F.col("runtime_seconds").isNull()
        ).count()

        print("\nRuntime quality:")
        print(f"Positive runtime rows: {positive_runtime_rows:,}")
        print(f"Zero runtime rows: {zero_runtime_rows:,}")
        print(f"Missing runtime rows: {missing_runtime_rows:,}")

        print("\nRuntime statistics for positive records:")
        (
            segments_df
            .filter(F.col("runtime_seconds") > 0)
            .select("runtime_minutes")
            .summary(
                "count",
                "mean",
                "stddev",
                "min",
                "25%",
                "50%",
                "75%",
                "max",
            )
            .show(truncate=False)
        )

        unique_routes = (
            segments_df
            .select("line_ref")
            .where(F.col("line_ref").isNotNull())
            .distinct()
            .count()
        )

        unique_stops = (
            segments_df
            .select(
                F.explode(
                    F.array("from_stop_ref", "to_stop_ref")
                ).alias("stop_ref")
            )
            .where(F.col("stop_ref").isNotNull())
            .distinct()
            .count()
        )

        unique_journeys = (
            segments_df
            .select(
                "source_file",
                "vehicle_journey_code",
            )
            .dropDuplicates()
            .count()
        )

        print("\nDataset coverage:")
        print(f"Unique routes: {unique_routes:,}")
        print(f"Unique stops: {unique_stops:,}")
        print(f"Unique vehicle journeys: {unique_journeys:,}")

        print("\nProfiling completed successfully.")
        print(
            "Keep the script running while viewing the Spark UI."
        )

        input(
            "Press Enter after taking the Spark UI screenshot..."
        )

    except Exception as error:
        print("\nProfiling failed.")
        print(f"Error type: {type(error).__name__}")
        print(f"Error message: {error}")
        raise

    finally:
        spark.stop()
        print("SparkSession stopped safely.")


if __name__ == "__main__":
    main()
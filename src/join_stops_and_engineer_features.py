from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from clean_segments import write_parquet_with_pyarrow


SEGMENT_FILE = Path(
    "data/processed/segments_cleaned.parquet"
)

STOP_FILE = Path(
    "data/interim/naptan_stagecoach_stops.csv"
)

OUTPUT_PARQUET = Path(
    "data/processed/segments_enriched.parquet"
)

SUMMARY_FILE = Path(
    "outputs/metrics/feature_engineering_summary.csv"
)

SAMPLE_FILE = Path(
    "outputs/metrics/enriched_segment_sample.csv"
)

EXECUTION_PLAN_FILE = Path(
    "outputs/metrics/stop_join_execution_plan.txt"
)

EARTH_RADIUS_KM = 6371.0088


def create_spark_session() -> SparkSession:
    """Create the Spark session for dataset integration."""

    return (
        SparkSession.builder
        .appName(
            "ST5011CEM_Stop_Join_Feature_Engineering"
        )
        .master("local[*]")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def build_stop_schema() -> StructType:
    """Define an explicit schema for the NaPTAN stop data."""

    return StructType(
        [
            StructField(
                "stop_ref",
                StringType(),
                True,
            ),
            StructField(
                "timetable_common_name",
                StringType(),
                True,
            ),
            StructField(
                "naptan_common_name",
                StringType(),
                True,
            ),
            StructField(
                "locality_name",
                StringType(),
                True,
            ),
            StructField(
                "town",
                StringType(),
                True,
            ),
            StructField(
                "latitude",
                DoubleType(),
                True,
            ),
            StructField(
                "longitude",
                DoubleType(),
                True,
            ),
            StructField(
                "has_coordinates",
                IntegerType(),
                True,
            ),
            StructField(
                "stop_type",
                StringType(),
                True,
            ),
            StructField(
                "status",
                StringType(),
                True,
            ),
            StructField(
                "administrative_area_code",
                StringType(),
                True,
            ),
            StructField(
                "atco_area_code",
                StringType(),
                True,
            ),
        ]
    )


def add_distance_features(
    dataframe: DataFrame,
) -> DataFrame:
    """
    Calculate straight-line distance between the origin
    and destination stops using the Haversine formula.
    """

    origin_latitude = F.radians(
        F.col("origin_latitude")
    )

    origin_longitude = F.radians(
        F.col("origin_longitude")
    )

    destination_latitude = F.radians(
        F.col("destination_latitude")
    )

    destination_longitude = F.radians(
        F.col("destination_longitude")
    )

    latitude_difference = (
        destination_latitude
        - origin_latitude
    )

    longitude_difference = (
        destination_longitude
        - origin_longitude
    )

    haversine_a = (
        F.pow(
            F.sin(
                latitude_difference / F.lit(2.0)
            ),
            2,
        )
        + F.cos(origin_latitude)
        * F.cos(destination_latitude)
        * F.pow(
            F.sin(
                longitude_difference / F.lit(2.0)
            ),
            2,
        )
    )

    safe_haversine_a = F.least(
        F.lit(1.0),
        F.greatest(
            F.lit(0.0),
            haversine_a,
        ),
    )

    distance_km = (
        F.lit(2.0 * EARTH_RADIUS_KM)
        * F.asin(
            F.sqrt(safe_haversine_a)
        )
    )

    return (
        dataframe
        .withColumn(
            "both_stops_matched",
            F.when(
                F.col("origin_latitude").isNotNull()
                & F.col("origin_longitude").isNotNull()
                & F.col(
                    "destination_latitude"
                ).isNotNull()
                & F.col(
                    "destination_longitude"
                ).isNotNull(),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "distance_km",
            F.when(
                F.col("both_stops_matched") == 1,
                distance_km,
            ).otherwise(
                F.lit(None).cast("double")
            ),
        )
        .withColumn(
            "distance_metres",
            F.round(
                F.col("distance_km")
                * F.lit(1000.0),
                2,
            ),
        )
        .withColumn(
            "sequence_gap",
            F.col("to_sequence")
            - F.col("from_sequence"),
        )
        .withColumn(
            "has_positive_distance",
            F.when(
                F.col("distance_km") > 0,
                1,
            ).otherwise(0),
        )
        .withColumn(
            "distance_band",
            F.when(
                F.col("distance_km").isNull(),
                "Missing",
            )
            .when(
                F.col("distance_km") == 0,
                "Zero",
            )
            .when(
                F.col("distance_km") < 0.25,
                "Under 0.25 km",
            )
            .when(
                F.col("distance_km") < 0.50,
                "0.25-0.50 km",
            )
            .when(
                F.col("distance_km") < 1.00,
                "0.50-1.00 km",
            )
            .when(
                F.col("distance_km") < 2.00,
                "1.00-2.00 km",
            )
            .otherwise("2.00 km or more"),
        )
    )


def save_summary(
    summary_rows: list[tuple[str, object]],
) -> None:
    """Save the feature-engineering summary."""

    SUMMARY_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with SUMMARY_FILE.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as output:
        writer = csv.writer(output)
        writer.writerow(
            ["metric", "value"]
        )
        writer.writerows(summary_rows)


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    cached_segments: DataFrame | None = None
    cached_stops: DataFrame | None = None
    cached_enriched: DataFrame | None = None

    try:
        print("\n" + "=" * 72)
        print(
            "PYSPARK DATASET INTEGRATION "
            "AND FEATURE ENGINEERING"
        )
        print("=" * 72)

        if not SEGMENT_FILE.exists():
            raise FileNotFoundError(
                "Cleaned segment dataset was not found: "
                f"{SEGMENT_FILE}"
            )

        if not STOP_FILE.exists():
            raise FileNotFoundError(
                "NaPTAN stop dataset was not found: "
                f"{STOP_FILE}"
            )

        cached_segments = (
            spark.read
            .parquet(str(SEGMENT_FILE))
            .repartition(4)
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        segment_count = cached_segments.count()

        cached_stops = (
            spark.read
            .option("header", True)
            .option("encoding", "UTF-8")
            .schema(build_stop_schema())
            .csv(str(STOP_FILE))
            .filter(
                F.col("stop_ref").isNotNull()
            )
            .dropDuplicates(
                ["stop_ref"]
            )
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        stop_count = cached_stops.count()

        distinct_stop_count = (
            cached_stops
            .select("stop_ref")
            .distinct()
            .count()
        )

        stops_with_coordinates = (
            cached_stops
            .filter(
                F.col("latitude").isNotNull()
                & F.col("longitude").isNotNull()
            )
            .count()
        )

        print(
            f"Segment records loaded: "
            f"{segment_count:,}"
        )

        print(
            f"NaPTAN stops loaded: "
            f"{stop_count:,}"
        )

        print(
            f"Distinct stop references: "
            f"{distinct_stop_count:,}"
        )

        print(
            f"Stops with coordinates: "
            f"{stops_with_coordinates:,}"
        )

        print(
            "Segment partitions: "
            f"{cached_segments.rdd.getNumPartitions()}"
        )

        # Create a small origin-stop lookup table.
        origin_lookup = F.broadcast(
            cached_stops.select(
                F.col("stop_ref").alias(
                    "origin_lookup_ref"
                ),
                F.col(
                    "naptan_common_name"
                ).alias(
                    "origin_stop_name"
                ),
                F.col(
                    "locality_name"
                ).alias(
                    "origin_locality_name"
                ),
                F.col("town").alias(
                    "origin_town"
                ),
                F.col("latitude").alias(
                    "origin_latitude"
                ),
                F.col("longitude").alias(
                    "origin_longitude"
                ),
                F.col("stop_type").alias(
                    "origin_stop_type"
                ),
                F.col("status").alias(
                    "origin_stop_status"
                ),
            )
        )

        # Create a separate destination-stop lookup.
        destination_lookup = F.broadcast(
            cached_stops.select(
                F.col("stop_ref").alias(
                    "destination_lookup_ref"
                ),
                F.col(
                    "naptan_common_name"
                ).alias(
                    "destination_stop_name"
                ),
                F.col(
                    "locality_name"
                ).alias(
                    "destination_locality_name"
                ),
                F.col("town").alias(
                    "destination_town"
                ),
                F.col("latitude").alias(
                    "destination_latitude"
                ),
                F.col("longitude").alias(
                    "destination_longitude"
                ),
                F.col("stop_type").alias(
                    "destination_stop_type"
                ),
                F.col("status").alias(
                    "destination_stop_status"
                ),
            )
        )

        joined_df = (
            cached_segments
            .join(
                origin_lookup,
                F.col("from_stop_ref")
                == F.col(
                    "origin_lookup_ref"
                ),
                "left",
            )
            .drop("origin_lookup_ref")
            .join(
                destination_lookup,
                F.col("to_stop_ref")
                == F.col(
                    "destination_lookup_ref"
                ),
                "left",
            )
            .drop("destination_lookup_ref")
        )

        joined_plan = (
            joined_df
            ._jdf
            .queryExecution()
            .executedPlan()
            .toString()
        )

        broadcast_join_detected = (
            "BroadcastHashJoin"
            in joined_plan
        )

        EXECUTION_PLAN_FILE.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        EXECUTION_PLAN_FILE.write_text(
            joined_plan,
            encoding="utf-8",
        )

        cached_enriched = (
            add_distance_features(
                joined_df
            )
            .repartition(4)
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        enriched_count = (
            cached_enriched.count()
        )

        origin_unmatched_count = (
            cached_enriched
            .filter(
                F.col(
                    "origin_latitude"
                ).isNull()
                | F.col(
                    "origin_longitude"
                ).isNull()
            )
            .count()
        )

        destination_unmatched_count = (
            cached_enriched
            .filter(
                F.col(
                    "destination_latitude"
                ).isNull()
                | F.col(
                    "destination_longitude"
                ).isNull()
            )
            .count()
        )

        both_stops_matched_count = (
            cached_enriched
            .filter(
                F.col(
                    "both_stops_matched"
                ) == 1
            )
            .count()
        )

        positive_distance_count = (
            cached_enriched
            .filter(
                F.col(
                    "has_positive_distance"
                ) == 1
            )
            .count()
        )

        zero_distance_count = (
            cached_enriched
            .filter(
                F.col("distance_km") == 0
            )
            .count()
        )

        missing_distance_count = (
            cached_enriched
            .filter(
                F.col(
                    "distance_km"
                ).isNull()
            )
            .count()
        )

        positive_distance_df = (
            cached_enriched
            .filter(
                F.col("distance_km") > 0
            )
        )

        distance_statistics = (
            positive_distance_df
            .agg(
                F.round(
                    F.avg("distance_km"),
                    6,
                ).alias(
                    "mean_distance_km"
                ),
                F.round(
                    F.stddev_samp(
                        "distance_km"
                    ),
                    6,
                ).alias(
                    "standard_deviation_distance_km"
                ),
                F.round(
                    F.min("distance_km"),
                    6,
                ).alias(
                    "minimum_distance_km"
                ),
                F.expr(
                    "percentile_approx("
                    "distance_km, 0.50, 10000)"
                ).alias(
                    "median_distance_km"
                ),
                F.expr(
                    "percentile_approx("
                    "distance_km, 0.95, 10000)"
                ).alias(
                    "percentile_95_distance_km"
                ),
                F.expr(
                    "percentile_approx("
                    "distance_km, 0.99, 10000)"
                ).alias(
                    "percentile_99_distance_km"
                ),
                F.round(
                    F.max("distance_km"),
                    6,
                ).alias(
                    "maximum_distance_km"
                ),
            )
            .first()
            .asDict()
        )

        match_percentage = (
            100.0
            * both_stops_matched_count
            / enriched_count
        )

        positive_distance_percentage = (
            100.0
            * positive_distance_count
            / enriched_count
        )

        print("\nJoin results:")
        print(
            f"Enriched records: "
            f"{enriched_count:,}"
        )
        print(
            f"Origin-stop unmatched records: "
            f"{origin_unmatched_count:,}"
        )
        print(
            "Destination-stop unmatched records: "
            f"{destination_unmatched_count:,}"
        )
        print(
            f"Both stops matched: "
            f"{both_stops_matched_count:,}"
        )
        print(
            f"Both-stop match percentage: "
            f"{match_percentage:.4f}%"
        )
        print(
            f"Broadcast join detected: "
            f"{broadcast_join_detected}"
        )

        print("\nDistance results:")
        print(
            f"Positive-distance records: "
            f"{positive_distance_count:,}"
        )
        print(
            f"Zero-distance records: "
            f"{zero_distance_count:,}"
        )
        print(
            f"Missing-distance records: "
            f"{missing_distance_count:,}"
        )

        for metric, value in (
            distance_statistics.items()
        ):
            print(
                f"{metric}: {value}"
            )

        print("\nSample enriched records:")

        cached_enriched.select(
            "line_ref",
            "from_stop_ref",
            "origin_stop_name",
            "to_stop_ref",
            "destination_stop_name",
            "distance_km",
            "distance_band",
            "runtime_minutes",
        ).show(
            10,
            truncate=False,
        )

        sample_pd: pd.DataFrame = (
            cached_enriched
            .select(
                "line_ref",
                "vehicle_journey_code",
                "departure_time",
                "from_stop_ref",
                "origin_stop_name",
                "origin_latitude",
                "origin_longitude",
                "to_stop_ref",
                "destination_stop_name",
                "destination_latitude",
                "destination_longitude",
                "distance_km",
                "distance_metres",
                "distance_band",
                "sequence_gap",
                "runtime_minutes",
            )
            .limit(100)
            .toPandas()
        )

        SAMPLE_FILE.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        sample_pd.to_csv(
            SAMPLE_FILE,
            index=False,
            encoding="utf-8",
        )

        print(
            "\nWriting enriched dataset "
            "to Parquet..."
        )

        rows_written = (
            write_parquet_with_pyarrow(
                dataframe=cached_enriched,
                output_path=OUTPUT_PARQUET,
                batch_size=20_000,
            )
        )

        if rows_written != enriched_count:
            raise RuntimeError(
                "Written Parquet row count does "
                "not match the enriched DataFrame. "
                f"Expected {enriched_count:,}, "
                f"wrote {rows_written:,}."
            )

        summary_rows = [
            (
                "input_segment_records",
                segment_count,
            ),
            (
                "naptan_stop_records",
                stop_count,
            ),
            (
                "distinct_stop_references",
                distinct_stop_count,
            ),
            (
                "stops_with_coordinates",
                stops_with_coordinates,
            ),
            (
                "enriched_segment_records",
                enriched_count,
            ),
            (
                "origin_unmatched_records",
                origin_unmatched_count,
            ),
            (
                "destination_unmatched_records",
                destination_unmatched_count,
            ),
            (
                "both_stops_matched_records",
                both_stops_matched_count,
            ),
            (
                "both_stops_match_percentage",
                round(
                    match_percentage,
                    4,
                ),
            ),
            (
                "positive_distance_records",
                positive_distance_count,
            ),
            (
                "positive_distance_percentage",
                round(
                    positive_distance_percentage,
                    4,
                ),
            ),
            (
                "zero_distance_records",
                zero_distance_count,
            ),
            (
                "missing_distance_records",
                missing_distance_count,
            ),
            (
                "broadcast_join_detected",
                broadcast_join_detected,
            ),
            (
                "final_spark_partitions",
                cached_enriched
                .rdd
                .getNumPartitions(),
            ),
            (
                "parquet_rows_written",
                rows_written,
            ),
        ]

        for metric, value in (
            distance_statistics.items()
        ):
            summary_rows.append(
                (metric, value)
            )

        save_summary(summary_rows)

        print("\nOutputs:")
        print(
            f"Enriched Parquet dataset: "
            f"{OUTPUT_PARQUET}"
        )
        print(
            f"Feature summary: "
            f"{SUMMARY_FILE}"
        )
        print(
            f"Enriched sample: "
            f"{SAMPLE_FILE}"
        )
        print(
            f"Spark join plan: "
            f"{EXECUTION_PLAN_FILE}"
        )
        print("=" * 72)
        print(
            "Dataset integration completed successfully."
        )

    except Exception as error:
        print(
            "\nDataset integration failed."
        )
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
        if cached_enriched is not None:
            cached_enriched.unpersist()

        if cached_stops is not None:
            cached_stops.unpersist()

        if cached_segments is not None:
            cached_segments.unpersist()

        spark.stop()
        print(
            "SparkSession stopped safely."
        )


if __name__ == "__main__":
    main()
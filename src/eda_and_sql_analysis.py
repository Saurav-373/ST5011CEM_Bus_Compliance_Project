from __future__ import annotations

import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import DataFrame, SparkSession


INPUT_PARQUET = Path(
    "data/processed/segments_cleaned.parquet"
)

METRICS_DIRECTORY = Path(
    "outputs/metrics"
)

FIGURES_DIRECTORY = Path(
    "outputs/figures"
)

EDA_SUMMARY_FILE = METRICS_DIRECTORY / "eda_summary.csv"

TIME_OF_DAY_FILE = (
    METRICS_DIRECTORY / "time_of_day_summary.csv"
)

PEAK_HOUR_FILE = (
    METRICS_DIRECTORY / "peak_hour_summary.csv"
)

ROUTE_SUMMARY_FILE = (
    METRICS_DIRECTORY / "route_runtime_summary.csv"
)

HIGH_DURATION_ROUTE_FILE = (
    METRICS_DIRECTORY / "high_duration_route_summary.csv"
)

TOP_HIGH_DURATION_FILE = (
    METRICS_DIRECTORY / "top_high_duration_segments.csv"
)


def create_spark_session() -> SparkSession:
    """Create the Spark session used for EDA."""

    return (
        SparkSession.builder
        .appName("ST5011CEM_EDA_SQL_Analysis")
        .master("local[*]")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def save_dataframe_as_csv(
    dataframe: pd.DataFrame,
    output_file: Path,
) -> None:
    """Save a small aggregated Pandas DataFrame as CSV."""

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    dataframe.to_csv(
        output_file,
        index=False,
        encoding="utf-8",
    )


def create_runtime_distribution_chart(
    distribution_df: pd.DataFrame,
) -> None:
    """Create the scheduled-runtime distribution chart."""

    chart_file = (
        FIGURES_DIRECTORY
        / "01_runtime_distribution.png"
    )

    plt.figure(figsize=(10, 6))

    plt.bar(
        distribution_df["runtime_minutes"],
        distribution_df["record_count"],
    )

    plt.title(
        "Distribution of Scheduled Segment Runtime"
    )
    plt.xlabel("Scheduled runtime in minutes")
    plt.ylabel("Number of segment records")
    plt.xticks(
        distribution_df["runtime_minutes"]
    )
    plt.tight_layout()
    plt.savefig(
        chart_file,
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def create_time_of_day_chart(
    time_df: pd.DataFrame,
) -> None:
    """Create the average runtime by time-of-day chart."""

    chart_file = (
        FIGURES_DIRECTORY
        / "02_average_runtime_by_time_of_day.png"
    )

    category_order = [
        "Morning",
        "Afternoon",
        "Evening",
        "Night",
    ]

    time_df["time_of_day"] = pd.Categorical(
        time_df["time_of_day"],
        categories=category_order,
        ordered=True,
    )

    time_df = time_df.sort_values(
        "time_of_day"
    )

    plt.figure(figsize=(9, 6))

    plt.bar(
        time_df["time_of_day"].astype(str),
        time_df["average_runtime_minutes"],
    )

    plt.title(
        "Average Scheduled Runtime by Time of Day"
    )
    plt.xlabel("Time of day")
    plt.ylabel("Average runtime in minutes")
    plt.tight_layout()
    plt.savefig(
        chart_file,
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def create_top_routes_chart(
    route_df: pd.DataFrame,
) -> None:
    """Create a chart for the ten largest routes."""

    chart_file = (
        FIGURES_DIRECTORY
        / "03_top_routes_by_segment_records.png"
    )

    top_routes = (
        route_df
        .sort_values(
            "record_count",
            ascending=False,
        )
        .head(10)
        .sort_values(
            "record_count",
            ascending=True,
        )
    )

    plt.figure(figsize=(11, 7))

    plt.barh(
        top_routes["line_ref"],
        top_routes["record_count"],
    )

    plt.title(
        "Top 10 Service Lines by Segment Records"
    )
    plt.xlabel("Number of segment records")
    plt.ylabel("Service line reference")
    plt.tight_layout()
    plt.savefig(
        chart_file,
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def create_high_duration_chart(
    high_duration_df: pd.DataFrame,
) -> None:
    """Create a chart showing high-duration shares."""

    chart_file = (
        FIGURES_DIRECTORY
        / "04_routes_with_high_duration_share.png"
    )

    top_routes = (
        high_duration_df
        .sort_values(
            "high_duration_percentage",
            ascending=False,
        )
        .head(10)
        .sort_values(
            "high_duration_percentage",
            ascending=True,
        )
    )

    plt.figure(figsize=(11, 7))

    plt.barh(
        top_routes["line_ref"],
        top_routes["high_duration_percentage"],
    )

    plt.title(
        "Routes with the Highest Share of Long Segments"
    )
    plt.xlabel(
        "High-duration records as a percentage"
    )
    plt.ylabel("Service line reference")
    plt.tight_layout()
    plt.savefig(
        chart_file,
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    cached_df: DataFrame | None = None

    try:
        print("\n" + "=" * 70)
        print("PYSPARK EXPLORATORY DATA ANALYSIS")
        print("=" * 70)

        if not INPUT_PARQUET.exists():
            raise FileNotFoundError(
                f"Input file was not found: {INPUT_PARQUET}"
            )

        METRICS_DIRECTORY.mkdir(
            parents=True,
            exist_ok=True,
        )

        FIGURES_DIRECTORY.mkdir(
            parents=True,
            exist_ok=True,
        )

        cached_df = (
            spark.read
            .parquet(str(INPUT_PARQUET))
            .repartition(4)
            .cache()
        )

        total_records = cached_df.count()

        print(f"Loaded records: {total_records:,}")
        print(
            "Spark partitions: "
            f"{cached_df.rdd.getNumPartitions()}"
        )
        print(f"Cached: {cached_df.is_cached}")

        cached_df.createOrReplaceTempView(
            "segments"
        )

        # -------------------------------------------------
        # 1. Overall descriptive statistics using Spark SQL
        # -------------------------------------------------

        overall_statistics = spark.sql(
            """
            SELECT
                COUNT(*) AS record_count,
                ROUND(AVG(runtime_minutes), 4)
                    AS mean_runtime_minutes,
                percentile_approx(
                    runtime_minutes,
                    0.50,
                    10000
                ) AS median_runtime_minutes,
                ROUND(
                    STDDEV_SAMP(runtime_minutes),
                    4
                ) AS standard_deviation_minutes,
                ROUND(
                    SKEWNESS(runtime_minutes),
                    4
                ) AS runtime_skewness,
                ROUND(
                    KURTOSIS(runtime_minutes),
                    4
                ) AS runtime_kurtosis,
                MIN(runtime_minutes)
                    AS minimum_runtime_minutes,
                percentile_approx(
                    runtime_minutes,
                    0.25,
                    10000
                ) AS first_quartile_minutes,
                percentile_approx(
                    runtime_minutes,
                    0.75,
                    10000
                ) AS third_quartile_minutes,
                percentile_approx(
                    runtime_minutes,
                    0.95,
                    10000
                ) AS percentile_95_minutes,
                percentile_approx(
                    runtime_minutes,
                    0.99,
                    10000
                ) AS percentile_99_minutes,
                MAX(runtime_minutes)
                    AS maximum_runtime_minutes,
                SUM(is_iqr_high_duration)
                    AS high_duration_records,
                ROUND(
                    100.0
                    * AVG(is_iqr_high_duration),
                    4
                ) AS high_duration_percentage,
                COUNT(DISTINCT line_ref)
                    AS unique_service_lines,
                COUNT(DISTINCT segment_key)
                    AS unique_segments,
                COUNT(
                DISTINCT source_file,
                vehicle_journey_code
                )
                    AS unique_vehicle_journeys
            FROM segments
            """
        )

        overall_row = (
            overall_statistics.first().asDict()
        )

        with EDA_SUMMARY_FILE.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as output:
            writer = csv.writer(output)
            writer.writerow(
                ["metric", "value"]
            )

            for metric, value in overall_row.items():
                writer.writerow(
                    [metric, value]
                )

        print("\nOverall runtime statistics:")

        for metric, value in overall_row.items():
            print(f"{metric}: {value}")

        # -----------------------------------------
        # 2. Runtime distribution using Spark SQL
        # -----------------------------------------

            duration_distribution = spark.sql(
                """
                WITH duration_counts AS (
                SELECT
                runtime_minutes,
                COUNT(*) AS record_count
            FROM segments
            GROUP BY runtime_minutes
        ),
        overall_total AS (
            SELECT
                SUM(record_count) AS total_records
            FROM duration_counts
        )
        SELECT
            duration_counts.runtime_minutes,
            duration_counts.record_count,
            ROUND(
                100.0
                * duration_counts.record_count
                / overall_total.total_records,
                4
            ) AS percentage
        FROM duration_counts
        CROSS JOIN overall_total
            ORDER BY duration_counts.runtime_minutes
                """
            )

        duration_distribution_pd = (
            duration_distribution.toPandas()
        )

        save_dataframe_as_csv(
            duration_distribution_pd,
            METRICS_DIRECTORY
            / "runtime_distribution.csv",
        )

        # -------------------------------------
        # 3. Time-of-day analysis using SQL
        # -------------------------------------

        time_of_day_summary = spark.sql(
            """
            SELECT
                time_of_day,
                COUNT(*) AS record_count,
                ROUND(
                    AVG(runtime_minutes),
                    4
                ) AS average_runtime_minutes,
                percentile_approx(
                    runtime_minutes,
                    0.50,
                    10000
                ) AS median_runtime_minutes,
                ROUND(
                    STDDEV_SAMP(runtime_minutes),
                    4
                ) AS standard_deviation_minutes,
                SUM(is_iqr_high_duration)
                    AS high_duration_records,
                ROUND(
                    100.0
                    * AVG(is_iqr_high_duration),
                    4
                ) AS high_duration_percentage
            FROM segments
            GROUP BY time_of_day
            ORDER BY
                CASE time_of_day
                    WHEN 'Morning' THEN 1
                    WHEN 'Afternoon' THEN 2
                    WHEN 'Evening' THEN 3
                    ELSE 4
                END
            """
        )

        time_of_day_pd = (
            time_of_day_summary.toPandas()
        )

        save_dataframe_as_csv(
            time_of_day_pd,
            TIME_OF_DAY_FILE,
        )

        # -------------------------------------
        # 4. Peak and off-peak SQL comparison
        # -------------------------------------

        peak_hour_summary = spark.sql(
            """
            SELECT
                CASE
                    WHEN is_peak_hour = 1
                        THEN 'Peak'
                    ELSE 'Off-peak'
                END AS period_type,
                COUNT(*) AS record_count,
                ROUND(
                    AVG(runtime_minutes),
                    4
                ) AS average_runtime_minutes,
                percentile_approx(
                    runtime_minutes,
                    0.50,
                    10000
                ) AS median_runtime_minutes,
                ROUND(
                    STDDEV_SAMP(runtime_minutes),
                    4
                ) AS standard_deviation_minutes,
                SUM(is_iqr_high_duration)
                    AS high_duration_records,
                ROUND(
                    100.0
                    * AVG(is_iqr_high_duration),
                    4
                ) AS high_duration_percentage
            FROM segments
            GROUP BY is_peak_hour
            ORDER BY is_peak_hour DESC
            """
        )

        peak_hour_pd = (
            peak_hour_summary.toPandas()
        )

        save_dataframe_as_csv(
            peak_hour_pd,
            PEAK_HOUR_FILE,
        )

        # -----------------------------------
        # 5. Route-level runtime statistics
        # -----------------------------------

        route_summary = spark.sql(
            """
            SELECT
                line_ref,
                COUNT(*) AS record_count,
                COUNT(DISTINCT segment_key)
                    AS unique_segments,
                COUNT(
                    DISTINCT source_file,
                    vehicle_journey_code
                ) AS unique_vehicle_journeys,
                ROUND(
                    AVG(runtime_minutes),
                    4
                ) AS average_runtime_minutes,
                percentile_approx(
                    runtime_minutes,
                    0.50,
                    10000
                ) AS median_runtime_minutes,
                ROUND(
                    STDDEV_SAMP(runtime_minutes),
                    4
                ) AS standard_deviation_minutes,
                MIN(runtime_minutes)
                    AS minimum_runtime_minutes,
                MAX(runtime_minutes)
                    AS maximum_runtime_minutes,
                SUM(is_iqr_high_duration)
                    AS high_duration_records,
                ROUND(
                    100.0
                    * AVG(is_iqr_high_duration),
                    4
                ) AS high_duration_percentage
            FROM segments
            GROUP BY line_ref
            ORDER BY record_count DESC
            """
        )

        route_summary_pd = (
            route_summary.toPandas()
        )

        save_dataframe_as_csv(
            route_summary_pd,
            ROUTE_SUMMARY_FILE,
        )

        # -----------------------------------------
        # 6. Route-level high-duration comparison
        # -----------------------------------------

        high_duration_route_summary = spark.sql(
            """
            SELECT
                line_ref,
                COUNT(*) AS record_count,
                SUM(is_iqr_high_duration)
                    AS high_duration_records,
                ROUND(
                    100.0
                    * AVG(is_iqr_high_duration),
                    4
                ) AS high_duration_percentage,
                ROUND(
                    AVG(runtime_minutes),
                    4
                ) AS average_runtime_minutes,
                MAX(runtime_minutes)
                    AS maximum_runtime_minutes
            FROM segments
            GROUP BY line_ref
            HAVING COUNT(*) >= 500
            ORDER BY
                high_duration_percentage DESC,
                record_count DESC
            """
        )

        high_duration_route_pd = (
            high_duration_route_summary.toPandas()
        )

        save_dataframe_as_csv(
            high_duration_route_pd,
            HIGH_DURATION_ROUTE_FILE,
        )

        # --------------------------------------
        # 7. Longest scheduled segment records
        # --------------------------------------

        top_high_duration = spark.sql(
            """
            SELECT
                line_ref,
                vehicle_journey_code,
                departure_time,
                time_of_day,
                from_stop_ref,
                to_stop_ref,
                runtime_minutes,
                is_iqr_high_duration
            FROM segments
            ORDER BY runtime_minutes DESC
            LIMIT 100
            """
        )

        top_high_duration_pd = (
            top_high_duration.toPandas()
        )

        save_dataframe_as_csv(
            top_high_duration_pd,
            TOP_HIGH_DURATION_FILE,
        )

        # ------------------
        # 8. Create figures
        # ------------------

        create_runtime_distribution_chart(
            duration_distribution_pd
        )

        create_time_of_day_chart(
            time_of_day_pd
        )

        create_top_routes_chart(
            route_summary_pd
        )

        create_high_duration_chart(
            high_duration_route_pd
        )

        print("\nGenerated CSV outputs:")

        for file_path in sorted(
            METRICS_DIRECTORY.glob("*.csv")
        ):
            print(f"- {file_path}")

        print("\nGenerated figures:")

        for file_path in sorted(
            FIGURES_DIRECTORY.glob("*.png")
        ):
            print(f"- {file_path}")

        print("\nEDA and SQL analysis completed successfully.")
        print("=" * 70)

    except Exception as error:
        print("\nEDA and SQL analysis failed.")
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
        if cached_df is not None:
            cached_df.unpersist()

        spark.stop()
        print("SparkSession stopped safely.")


if __name__ == "__main__":
    main()
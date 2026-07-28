from __future__ import annotations

import csv
import time
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import (
    OneHotEncoder,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.regression import (
    DecisionTreeRegressor,
    GBTRegressor,
    LinearRegression,
    RandomForestRegressor,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from clean_segments import write_parquet_with_pyarrow


INPUT_PARQUET = Path(
    "data/processed/segments_enriched.parquet"
)

METRICS_DIRECTORY = Path(
    "outputs/metrics"
)

FIGURES_DIRECTORY = Path(
    "outputs/figures"
)

PREDICTIONS_DIRECTORY = Path(
    "outputs/predictions"
)

MODEL_COMPARISON_FILE = (
    METRICS_DIRECTORY / "model_comparison.csv"
)

SPLIT_SUMMARY_FILE = (
    METRICS_DIRECTORY / "model_split_summary.csv"
)

BEST_MODEL_SUMMARY_FILE = (
    METRICS_DIRECTORY / "best_model_summary.csv"
)

FEATURE_IMPORTANCE_FILE = (
    METRICS_DIRECTORY / "best_model_feature_importance.csv"
)

ANOMALY_SUMMARY_FILE = (
    METRICS_DIRECTORY / "model_anomaly_summary.csv"
)

TOP_ANOMALIES_FILE = (
    METRICS_DIRECTORY / "top_model_anomalies.csv"
)

TEST_PREDICTIONS_FILE = (
    PREDICTIONS_DIRECTORY / "test_predictions.parquet"
)

RANDOM_SEED = 42


CATEGORICAL_FEATURES = [
    "line_ref",
    "time_of_day",
    "distance_band",
    "from_timing_status",
    "to_timing_status",
    "from_activity",
    "to_activity",
    "origin_stop_type",
    "destination_stop_type",
]


NUMERIC_FEATURES = [
    "distance_km",
    "departure_hour",
    "is_peak_hour",
    "from_sequence",
    "sequence_gap",
    "origin_latitude",
    "origin_longitude",
    "destination_latitude",
    "destination_longitude",
]


IDENTIFIER_COLUMNS = [
    "source_file",
    "vehicle_journey_code",
    "line_ref",
    "segment_key",
    "departure_time",
    "from_stop_ref",
    "origin_stop_name",
    "to_stop_ref",
    "destination_stop_name",
    "distance_km",
    "runtime_minutes",
]


def create_spark_session() -> SparkSession:
    """Create the Spark session for model training."""

    return (
        SparkSession.builder
        .appName(
            "ST5011CEM_Runtime_Model_Comparison"
        )
        .master("local[*]")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def save_metric_rows(
    output_file: Path,
    rows: list[tuple[str, Any]],
) -> None:
    """Save metric-value pairs as a CSV file."""

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


def extract_feature_names(
    dataframe: DataFrame,
    feature_column: str = "features",
) -> list[str]:
    """Extract assembled feature names from Spark metadata."""

    metadata = dataframe.schema[
        feature_column
    ].metadata

    ml_attributes = (
        metadata
        .get("ml_attr", {})
        .get("attrs", {})
    )

    indexed_names: list[
        tuple[int, str]
    ] = []

    for attribute_type in [
        "numeric",
        "binary",
        "nominal",
    ]:
        for attribute in ml_attributes.get(
            attribute_type,
            [],
        ):
            indexed_names.append(
                (
                    int(attribute["idx"]),
                    str(
                        attribute.get(
                            "name",
                            f"feature_{attribute['idx']}",
                        )
                    ),
                )
            )

    indexed_names.sort(
        key=lambda item: item[0]
    )

    return [
        feature_name
        for _, feature_name in indexed_names
    ]


def create_model_comparison_charts(
    metrics_df: pd.DataFrame,
) -> None:
    """Create separate RMSE and R-squared charts."""

    rmse_file = (
        FIGURES_DIRECTORY
        / "05_model_rmse_comparison.png"
    )

    r2_file = (
        FIGURES_DIRECTORY
        / "06_model_r2_comparison.png"
    )

    ordered_rmse = metrics_df.sort_values(
        "rmse",
        ascending=True,
    )

    plt.figure(figsize=(10, 6))
    plt.barh(
        ordered_rmse["model_name"],
        ordered_rmse["rmse"],
    )
    plt.title(
        "Regression Model RMSE Comparison"
    )
    plt.xlabel(
        "Root Mean Squared Error (minutes)"
    )
    plt.ylabel("Model")
    plt.tight_layout()
    plt.savefig(
        rmse_file,
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()

    ordered_r2 = metrics_df.sort_values(
        "r2",
        ascending=True,
    )

    plt.figure(figsize=(10, 6))
    plt.barh(
        ordered_r2["model_name"],
        ordered_r2["r2"],
    )
    plt.title(
        "Regression Model R² Comparison"
    )
    plt.xlabel("R² score")
    plt.ylabel("Model")
    plt.tight_layout()
    plt.savefig(
        r2_file,
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def create_residual_chart(
    residual_sample: pd.DataFrame,
) -> None:
    """Create a residual-distribution chart."""

    output_file = (
        FIGURES_DIRECTORY
        / "07_best_model_residual_distribution.png"
    )

    plt.figure(figsize=(10, 6))
    plt.hist(
        residual_sample["residual_minutes"],
        bins=40,
    )
    plt.title(
        "Best Model Residual Distribution"
    )
    plt.xlabel(
        "Residual: actual minus predicted runtime"
    )
    plt.ylabel("Number of test records")
    plt.tight_layout()
    plt.savefig(
        output_file,
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    cached_source: DataFrame | None = None
    cached_train: DataFrame | None = None
    cached_test: DataFrame | None = None
    cached_train_prepared: DataFrame | None = None
    cached_test_prepared: DataFrame | None = None
    cached_train_predictions: DataFrame | None = None
    cached_test_predictions: DataFrame | None = None

    try:
        print("\n" + "=" * 74)
        print(
            "PYSPARK REGRESSION MODEL TRAINING "
            "AND COMPARISON"
        )
        print("=" * 74)

        if not INPUT_PARQUET.exists():
            raise FileNotFoundError(
                "Enriched Parquet dataset was not found: "
                f"{INPUT_PARQUET}"
            )

        METRICS_DIRECTORY.mkdir(
            parents=True,
            exist_ok=True,
        )

        FIGURES_DIRECTORY.mkdir(
            parents=True,
            exist_ok=True,
        )

        PREDICTIONS_DIRECTORY.mkdir(
            parents=True,
            exist_ok=True,
        )

        cached_source = (
            spark.read
            .parquet(str(INPUT_PARQUET))
            .repartition(4)
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        source_count = cached_source.count()

        required_columns = set(
            CATEGORICAL_FEATURES
            + NUMERIC_FEATURES
            + IDENTIFIER_COLUMNS
            + ["segment_key"]
        )

        missing_columns = sorted(
            required_columns
            - set(cached_source.columns)
        )

        if missing_columns:
            raise ValueError(
                "The enriched dataset is missing columns: "
                + ", ".join(missing_columns)
            )

        print(
            f"Source records loaded: "
            f"{source_count:,}"
        )

        print(
            "Source Spark partitions: "
            f"{cached_source.rdd.getNumPartitions()}"
        )

        # Only valid positive targets and distances are used.
        modelling_df = (
            cached_source
            .filter(
                F.col("runtime_minutes") > 0
            )
            .filter(
                F.col("distance_km") > 0
            )
            .withColumn(
                "label",
                F.col(
                    "runtime_minutes"
                ).cast("double"),
            )
            .withColumn(
                "split_bucket",
                F.pmod(
                    F.xxhash64(
                        F.col("segment_key")
                    ),
                    F.lit(100),
                ),
            )
        )

        # All copies of a segment remain in one split.
        cached_train = (
            modelling_df
            .filter(
                F.col("split_bucket") < 80
            )
            .drop("split_bucket")
            .repartition(4)
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        cached_test = (
            modelling_df
            .filter(
                F.col("split_bucket") >= 80
            )
            .drop("split_bucket")
            .repartition(4)
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        train_count = cached_train.count()
        test_count = cached_test.count()

        train_segment_count = (
            cached_train
            .select("segment_key")
            .distinct()
            .count()
        )

        test_segment_count = (
            cached_test
            .select("segment_key")
            .distinct()
            .count()
        )

        overlapping_segments = (
            cached_train
            .select("segment_key")
            .distinct()
            .join(
                cached_test
                .select("segment_key")
                .distinct(),
                on="segment_key",
                how="inner",
            )
            .count()
        )

        train_percentage = (
            100.0
            * train_count
            / (train_count + test_count)
        )

        test_percentage = (
            100.0
            * test_count
            / (train_count + test_count)
        )

        print("\nLeakage-safe split:")
        print(
            f"Training records: "
            f"{train_count:,} "
            f"({train_percentage:.2f}%)"
        )
        print(
            f"Testing records: "
            f"{test_count:,} "
            f"({test_percentage:.2f}%)"
        )
        print(
            f"Training segments: "
            f"{train_segment_count:,}"
        )
        print(
            f"Testing segments: "
            f"{test_segment_count:,}"
        )
        print(
            f"Overlapping segments: "
            f"{overlapping_segments:,}"
        )

        if overlapping_segments != 0:
            raise RuntimeError(
                "Segment leakage was detected between "
                "training and testing data."
            )

        save_metric_rows(
            SPLIT_SUMMARY_FILE,
            [
                (
                    "source_records",
                    source_count,
                ),
                (
                    "training_records",
                    train_count,
                ),
                (
                    "testing_records",
                    test_count,
                ),
                (
                    "training_percentage",
                    round(
                        train_percentage,
                        4,
                    ),
                ),
                (
                    "testing_percentage",
                    round(
                        test_percentage,
                        4,
                    ),
                ),
                (
                    "training_unique_segments",
                    train_segment_count,
                ),
                (
                    "testing_unique_segments",
                    test_segment_count,
                ),
                (
                    "overlapping_segments",
                    overlapping_segments,
                ),
                (
                    "training_spark_partitions",
                    cached_train
                    .rdd
                    .getNumPartitions(),
                ),
                (
                    "testing_spark_partitions",
                    cached_test
                    .rdd
                    .getNumPartitions(),
                ),
            ],
        )

        indexed_columns = [
            f"{column_name}_index"
            for column_name
            in CATEGORICAL_FEATURES
        ]

        encoded_columns = [
            f"{column_name}_encoded"
            for column_name
            in CATEGORICAL_FEATURES
        ]

        indexers = [
            StringIndexer(
                inputCol=column_name,
                outputCol=indexed_column,
                handleInvalid="keep",
            )
            for column_name, indexed_column
            in zip(
                CATEGORICAL_FEATURES,
                indexed_columns,
            )
        ]

        encoder = OneHotEncoder(
            inputCols=indexed_columns,
            outputCols=encoded_columns,
            handleInvalid="keep",
            dropLast=True,
        )

        assembler = VectorAssembler(
            inputCols=(
                NUMERIC_FEATURES
                + encoded_columns
            ),
            outputCol="features",
            handleInvalid="keep",
        )

        preprocessing_pipeline = Pipeline(
            stages=(
                indexers
                + [
                    encoder,
                    assembler,
                ]
            )
        )

        print(
            "\nFitting preprocessing pipeline "
            "on training data..."
        )

        preprocessing_model = (
            preprocessing_pipeline.fit(
                cached_train
            )
        )

        prepared_columns = list(
            dict.fromkeys(
                IDENTIFIER_COLUMNS
                + [
                    "label",
                    "features",
                ]
            )
        )

        cached_train_prepared = (
            preprocessing_model
            .transform(cached_train)
            .select(*prepared_columns)
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        cached_test_prepared = (
            preprocessing_model
            .transform(cached_test)
            .select(*prepared_columns)
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        prepared_train_count = (
            cached_train_prepared.count()
        )

        prepared_test_count = (
            cached_test_prepared.count()
        )

        feature_names = extract_feature_names(
            cached_train_prepared
        )

        feature_count = len(feature_names)

        print(
            f"Prepared training records: "
            f"{prepared_train_count:,}"
        )
        print(
            f"Prepared testing records: "
            f"{prepared_test_count:,}"
        )
        print(
            f"Final feature-vector size: "
            f"{feature_count:,}"
        )

        candidate_models = [
            (
                "Linear Regression",
                (
                    "regParam=0.05; "
                    "elasticNetParam=0.0; "
                    "maxIter=50"
                ),
                LinearRegression(
                    featuresCol="features",
                    labelCol="label",
                    predictionCol="prediction",
                    regParam=0.05,
                    elasticNetParam=0.0,
                    maxIter=50,
                ),
            ),
            (
                "Decision Tree",
                (
                    "maxDepth=10; "
                    "minInstancesPerNode=20"
                ),
                DecisionTreeRegressor(
                    featuresCol="features",
                    labelCol="label",
                    predictionCol="prediction",
                    maxDepth=10,
                    minInstancesPerNode=20,
                    seed=RANDOM_SEED,
                ),
            ),
            (
                "Random Forest",
                (
                    "numTrees=40; "
                    "maxDepth=10; "
                    "featureSubsetStrategy=sqrt; "
                    "subsamplingRate=0.8"
                ),
                RandomForestRegressor(
                    featuresCol="features",
                    labelCol="label",
                    predictionCol="prediction",
                    numTrees=40,
                    maxDepth=10,
                    featureSubsetStrategy="sqrt",
                    subsamplingRate=0.8,
                    seed=RANDOM_SEED,
                ),
            ),
            (
                "Gradient-Boosted Trees",
                (
                    "maxIter=20; "
                    "maxDepth=6; "
                    "stepSize=0.1; "
                    "subsamplingRate=0.8"
                ),
                GBTRegressor(
                    featuresCol="features",
                    labelCol="label",
                    predictionCol="prediction",
                    maxIter=20,
                    maxDepth=6,
                    stepSize=0.1,
                    subsamplingRate=0.8,
                    seed=RANDOM_SEED,
                ),
            ),
        ]

        rmse_evaluator = RegressionEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="rmse",
        )

        mae_evaluator = RegressionEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="mae",
        )

        r2_evaluator = RegressionEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="r2",
        )

        model_results: list[
            dict[str, Any]
        ] = []

        best_model = None
        best_model_name = ""
        best_model_configuration = ""
        best_rmse = float("inf")

        print("\nTraining regression models:")

        for (
            model_name,
            configuration,
            estimator,
        ) in candidate_models:
            print(
                f"\nTraining {model_name}..."
            )

            training_start = (
                time.perf_counter()
            )

            fitted_model = estimator.fit(
                cached_train_prepared
            )

            training_seconds = (
                time.perf_counter()
                - training_start
            )

            predictions = (
                fitted_model
                .transform(
                    cached_test_prepared
                )
                .persist(
                    StorageLevel.MEMORY_AND_DISK
                )
            )

            prediction_count = (
                predictions.count()
            )

            evaluation_start = (
                time.perf_counter()
            )

            rmse = rmse_evaluator.evaluate(
                predictions
            )

            mae = mae_evaluator.evaluate(
                predictions
            )

            r2 = r2_evaluator.evaluate(
                predictions
            )

            evaluation_seconds = (
                time.perf_counter()
                - evaluation_start
            )

            result = {
                "model_name": model_name,
                "configuration": configuration,
                "training_records": (
                    prepared_train_count
                ),
                "testing_records": (
                    prediction_count
                ),
                "rmse": round(
                    rmse,
                    6,
                ),
                "mae": round(
                    mae,
                    6,
                ),
                "r2": round(
                    r2,
                    6,
                ),
                "training_seconds": round(
                    training_seconds,
                    4,
                ),
                "evaluation_seconds": round(
                    evaluation_seconds,
                    4,
                ),
            }

            model_results.append(result)

            print(
                f"RMSE: {rmse:.6f}"
            )
            print(
                f"MAE: {mae:.6f}"
            )
            print(
                f"R²: {r2:.6f}"
            )
            print(
                "Training time: "
                f"{training_seconds:.2f} seconds"
            )

            if rmse < best_rmse:
                best_rmse = rmse
                best_model = fitted_model
                best_model_name = model_name
                best_model_configuration = (
                    configuration
                )

            predictions.unpersist()

        if best_model is None:
            raise RuntimeError(
                "No model was successfully trained."
            )

        model_results_df = (
            pd.DataFrame(model_results)
            .sort_values(
                "rmse",
                ascending=True,
            )
            .reset_index(drop=True)
        )

        model_results_df.insert(
            0,
            "rank_by_rmse",
            range(
                1,
                len(model_results_df) + 1,
            ),
        )

        model_results_df.to_csv(
            MODEL_COMPARISON_FILE,
            index=False,
            encoding="utf-8",
        )

        create_model_comparison_charts(
            model_results_df
        )

        best_result = (
            model_results_df.iloc[0]
        )

        print("\n" + "-" * 74)
        print("BEST MODEL")
        print("-" * 74)
        print(
            f"Model: {best_model_name}"
        )
        print(
            f"RMSE: {best_result['rmse']}"
        )
        print(
            f"MAE: {best_result['mae']}"
        )
        print(
            f"R²: {best_result['r2']}"
        )

        # A residual threshold is learned only from training data.
        cached_train_predictions = (
            best_model
            .transform(
                cached_train_prepared
            )
            .withColumn(
                "residual_minutes",
                F.col("label")
                - F.col("prediction"),
            )
            .withColumn(
                "absolute_error_minutes",
                F.abs(
                    F.col(
                        "residual_minutes"
                    )
                ),
            )
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        cached_train_predictions.count()

        residual_threshold = (
            cached_train_predictions
            .approxQuantile(
                "absolute_error_minutes",
                [0.95],
                0.001,
            )[0]
        )

        cached_test_predictions = (
            best_model
            .transform(
                cached_test_prepared
            )
            .withColumn(
                "residual_minutes",
                F.col("label")
                - F.col("prediction"),
            )
            .withColumn(
                "absolute_error_minutes",
                F.abs(
                    F.col(
                        "residual_minutes"
                    )
                ),
            )
            .withColumn(
                "is_model_anomaly",
                F.when(
                    F.col(
                        "absolute_error_minutes"
                    )
                    > F.lit(
                        residual_threshold
                    ),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "anomaly_direction",
                F.when(
                    F.col("is_model_anomaly")
                    == 0,
                    "Within expected range",
                )
                .when(
                    F.col(
                        "residual_minutes"
                    ) > 0,
                    "Longer than expected",
                )
                .otherwise(
                    "Shorter than expected"
                ),
            )
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        test_prediction_count = (
            cached_test_predictions.count()
        )

        anomaly_count = (
            cached_test_predictions
            .filter(
                F.col("is_model_anomaly") == 1
            )
            .count()
        )

        longer_anomaly_count = (
            cached_test_predictions
            .filter(
                F.col("anomaly_direction")
                == "Longer than expected"
            )
            .count()
        )

        shorter_anomaly_count = (
            cached_test_predictions
            .filter(
                F.col("anomaly_direction")
                == "Shorter than expected"
            )
            .count()
        )

        anomaly_percentage = (
            100.0
            * anomaly_count
            / test_prediction_count
        )

        print("\nResidual-based anomaly analysis:")
        print(
            "Training-derived 95th percentile "
            "absolute-error threshold: "
            f"{residual_threshold:.6f} minutes"
        )
        print(
            f"Test anomalies: "
            f"{anomaly_count:,}"
        )
        print(
            f"Test anomaly percentage: "
            f"{anomaly_percentage:.4f}%"
        )
        print(
            "Longer-than-expected anomalies: "
            f"{longer_anomaly_count:,}"
        )
        print(
            "Shorter-than-expected anomalies: "
            f"{shorter_anomaly_count:,}"
        )

        prediction_output_columns = [
            "source_file",
            "vehicle_journey_code",
            "line_ref",
            "segment_key",
            "departure_time",
            "from_stop_ref",
            "origin_stop_name",
            "to_stop_ref",
            "destination_stop_name",
            "distance_km",
            "runtime_minutes",
            "prediction",
            "residual_minutes",
            "absolute_error_minutes",
            "is_model_anomaly",
            "anomaly_direction",
        ]

        prediction_output_df = (
            cached_test_predictions
            .select(
                *prediction_output_columns
            )
            .repartition(4)
        )

        written_prediction_rows = (
            write_parquet_with_pyarrow(
                dataframe=prediction_output_df,
                output_path=(
                    TEST_PREDICTIONS_FILE
                ),
                batch_size=20_000,
            )
        )

        if (
            written_prediction_rows
            != test_prediction_count
        ):
            raise RuntimeError(
                "Prediction output row count mismatch. "
                f"Expected {test_prediction_count:,}, "
                f"wrote "
                f"{written_prediction_rows:,}."
            )

        top_anomalies_pd = (
            cached_test_predictions
            .filter(
                F.col("is_model_anomaly") == 1
            )
            .select(
                *prediction_output_columns
            )
            .orderBy(
                F.col(
                    "absolute_error_minutes"
                ).desc()
            )
            .limit(100)
            .toPandas()
        )

        top_anomalies_pd.to_csv(
            TOP_ANOMALIES_FILE,
            index=False,
            encoding="utf-8",
        )

        residual_sample_pd = (
            cached_test_predictions
            .select("residual_minutes")
            .sample(
                withReplacement=False,
                fraction=0.20,
                seed=RANDOM_SEED,
            )
            .limit(20_000)
            .toPandas()
        )

        create_residual_chart(
            residual_sample_pd
        )

        save_metric_rows(
            BEST_MODEL_SUMMARY_FILE,
            [
                (
                    "best_model_name",
                    best_model_name,
                ),
                (
                    "best_model_configuration",
                    best_model_configuration,
                ),
                (
                    "best_model_rmse",
                    best_result["rmse"],
                ),
                (
                    "best_model_mae",
                    best_result["mae"],
                ),
                (
                    "best_model_r2",
                    best_result["r2"],
                ),
                (
                    "feature_vector_size",
                    feature_count,
                ),
                (
                    "training_records",
                    prepared_train_count,
                ),
                (
                    "testing_records",
                    prepared_test_count,
                ),
            ],
        )

        save_metric_rows(
            ANOMALY_SUMMARY_FILE,
            [
                (
                    "best_model_name",
                    best_model_name,
                ),
                (
                    "test_prediction_records",
                    test_prediction_count,
                ),
                (
                    "training_absolute_error_95th_percentile",
                    residual_threshold,
                ),
                (
                    "model_anomaly_records",
                    anomaly_count,
                ),
                (
                    "model_anomaly_percentage",
                    round(
                        anomaly_percentage,
                        4,
                    ),
                ),
                (
                    "longer_than_expected_anomalies",
                    longer_anomaly_count,
                ),
                (
                    "shorter_than_expected_anomalies",
                    shorter_anomaly_count,
                ),
                (
                    "prediction_rows_written",
                    written_prediction_rows,
                ),
            ],
        )

        feature_scores: list[
            tuple[str, float]
        ] = []

        if hasattr(
            best_model,
            "featureImportances",
        ):
            importance_values = (
                best_model
                .featureImportances
                .toArray()
            )

            for index, importance in enumerate(
                importance_values
            ):
                feature_name = (
                    feature_names[index]
                    if index
                    < len(feature_names)
                    else f"feature_{index}"
                )

                feature_scores.append(
                    (
                        feature_name,
                        float(importance),
                    )
                )

            score_column_name = "importance"

        elif hasattr(
            best_model,
            "coefficients",
        ):
            coefficient_values = (
                best_model
                .coefficients
                .toArray()
            )

            for index, coefficient in enumerate(
                coefficient_values
            ):
                feature_name = (
                    feature_names[index]
                    if index
                    < len(feature_names)
                    else f"feature_{index}"
                )

                feature_scores.append(
                    (
                        feature_name,
                        float(
                            abs(coefficient)
                        ),
                    )
                )

            score_column_name = (
                "absolute_coefficient"
            )

        else:
            score_column_name = "score"

        feature_scores.sort(
            key=lambda item: item[1],
            reverse=True,
        )

        with FEATURE_IMPORTANCE_FILE.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as output:
            writer = csv.writer(output)
            writer.writerow(
                [
                    "rank",
                    "feature_name",
                    score_column_name,
                ]
            )

            for rank, (
                feature_name,
                score,
            ) in enumerate(
                feature_scores,
                start=1,
            ):
                writer.writerow(
                    [
                        rank,
                        feature_name,
                        score,
                    ]
                )

        print("\nOutputs:")
        print(
            f"Model comparison: "
            f"{MODEL_COMPARISON_FILE}"
        )
        print(
            f"Split summary: "
            f"{SPLIT_SUMMARY_FILE}"
        )
        print(
            f"Best-model summary: "
            f"{BEST_MODEL_SUMMARY_FILE}"
        )
        print(
            f"Anomaly summary: "
            f"{ANOMALY_SUMMARY_FILE}"
        )
        print(
            f"Feature importance: "
            f"{FEATURE_IMPORTANCE_FILE}"
        )
        print(
            f"Top anomalies: "
            f"{TOP_ANOMALIES_FILE}"
        )
        print(
            f"Test predictions: "
            f"{TEST_PREDICTIONS_FILE}"
        )
        print("=" * 74)
        print(
            "Model training and comparison "
            "completed successfully."
        )

    except Exception as error:
        print(
            "\nModel training failed."
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
        for dataframe in [
            cached_test_predictions,
            cached_train_predictions,
            cached_test_prepared,
            cached_train_prepared,
            cached_test,
            cached_train,
            cached_source,
        ]:
            if dataframe is not None:
                dataframe.unpersist()

        spark.stop()
        print(
            "SparkSession stopped safely."
        )


if __name__ == "__main__":
    main()
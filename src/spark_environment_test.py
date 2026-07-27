from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main() -> None:
    """Test the local PySpark installation and partition configuration."""

    spark = None

    try:
        spark = (
            SparkSession.builder
            .appName("ST5011CEM_Spark_Environment_Test")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.default.parallelism", "4")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        print("\n" + "=" * 60)
        print("PYSPARK ENVIRONMENT TEST")
        print("=" * 60)
        print(f"Spark version: {spark.version}")
        print(f"Spark master: {spark.sparkContext.master}")
        print(f"Application ID: {spark.sparkContext.applicationId}")
        print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")
        print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
        print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
        print("=" * 60)

        # Create a distributed DataFrame containing 100,000 rows.
        test_df = (
            spark.range(
                start=0,
                end=100_000,
                step=1,
                numPartitions=4
            )
            .withColumn("square", col("id") * col("id"))
        )

        print(f"\nOriginal partition count: {test_df.rdd.getNumPartitions()}")

        # Cache the DataFrame because it will be reused.
        test_df.cache()

        total_rows = test_df.count()
        print(f"Total record count: {total_rows:,}")
        print(f"Cached status: {test_df.is_cached}")

        # Perform a distributed transformation and aggregation.
        result_df = (
            test_df
            .filter(col("id") % 2 == 0)
            .withColumn("group_number", col("id") % 10)
            .groupBy("group_number")
            .count()
            .orderBy("group_number")
        )

        print(
            f"Result partition count: "
            f"{result_df.rdd.getNumPartitions()}"
        )

        print("\nAggregation result:")
        result_df.show(truncate=False)

        print("\nSpark test completed successfully.")
        print("Keep this terminal open and inspect the Spark UI.")
        input("Press Enter when you are ready to stop Spark...")

    except Exception as error:
        print("\nSpark test failed.")
        print(f"Error type: {type(error).__name__}")
        print(f"Error message: {error}")
        raise

    finally:
        if spark is not None:
            spark.stop()
            print("SparkSession stopped safely.")


if __name__ == "__main__":
    main()
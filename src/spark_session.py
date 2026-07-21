# Spark session setup for local development.
# This creates a reusable Spark context for all data-processing steps.
from pyspark.sql import SparkSession


def get_spark_session():
    """
    Create and return a Spark session for the Banking Transaction Fraud Analysis project.
    
    Returns:
        SparkSession: Configured Spark session instance
    """
    spark = SparkSession.builder \
        .appName("BankingTransactionFraudAnalysis") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


if __name__ == "__main__":
    # Test the Spark session
    spark = get_spark_session()
    print(f"Spark Version: {spark.version}")
    print(f"App Name: {spark.sparkContext.appName}")
    print("Spark session created successfully!")

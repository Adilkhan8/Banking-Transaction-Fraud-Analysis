from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, trim, upper

from spark_session import get_spark_session


def _get_data_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "Data" / "raw"


def _read_csv(spark: SparkSession, filename: str) -> DataFrame:
    path = _get_data_dir() / filename
    return spark.read.option("header", True) \
        .option("inferSchema", True) \
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
        .csv(str(path))


def clean_transactions(df: DataFrame) -> DataFrame:
    """Clean raw transaction data from CSV before downstream processing."""
    cleaned = df.withColumn("transaction_id", trim(col("transaction_id"))) \
        .withColumn("account_id", trim(col("account_id"))) \
        .withColumn("customer_id", trim(col("account_id"))) \
        .withColumn("transaction_time", col("timestamp").cast("timestamp")) \
        .withColumn("merchant", trim(col("merchant"))) \
        .withColumn("transaction_type", trim(upper(col("transaction_type")))) \
        .withColumn("amount", col("amount").cast("double"))

    cleaned = cleaned.filter(
        col("transaction_id").isNotNull() &
        col("account_id").isNotNull() &
        col("transaction_time").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0)
    )

    return cleaned.dropDuplicates(["transaction_id"])


def clean_customers(df: DataFrame) -> DataFrame:
    """Clean raw customer data from CSV."""
    cleaned = df.withColumn("account_id", trim(col("account_id"))) \
        .withColumn("customer_name", trim(col("customer_name"))) \
        .withColumn("city", trim(col("city"))) \
        .withColumn("age", col("age").cast("int"))

    return cleaned.filter(
        col("account_id").isNotNull() &
        col("customer_name").isNotNull() &
        col("city").isNotNull() &
        col("age").isNotNull() &
        (col("age") > 0)
    ).dropDuplicates(["account_id"])


def clean_merchants(df: DataFrame) -> DataFrame:
    """Clean raw merchant data from CSV."""
    cleaned = df.withColumn("merchant_id", trim(col("merchant_id"))) \
        .withColumn("merchant_name", trim(col("merchant_name"))) \
        .withColumn("category", trim(col("category")))

    return cleaned.filter(
        col("merchant_id").isNotNull() &
        col("merchant_name").isNotNull() &
        col("category").isNotNull()
    ).dropDuplicates(["merchant_id"])


def ingest_data() -> tuple[DataFrame, DataFrame, DataFrame]:
    """Load and cleanse transaction, customer, and merchant CSV files."""
    spark = get_spark_session()

    transactions_raw = _read_csv(spark, "banking_transactions_20000.csv")
    customers_raw = _read_csv(spark, "customers_1000.csv")
    merchants_raw = _read_csv(spark, "merchants.csv")

    transactions = clean_transactions(transactions_raw)
    customers = clean_customers(customers_raw)
    merchants = clean_merchants(merchants_raw)

    return transactions, customers, merchants


if __name__ == "__main__":
    transactions, customers, merchants = ingest_data()

    print("Cleaned transaction count:", transactions.count())
    print("Cleaned customer count:", customers.count())
    print("Cleaned merchant count:", merchants.count())

    transactions.printSchema()
    transactions.show(5, truncate=False)

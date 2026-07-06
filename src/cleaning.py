# Cleaning utilities for raw banking datasets.
# These functions standardize column values and remove invalid or duplicate rows.
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper


def clean_transactions(df: DataFrame) -> DataFrame:
    """Clean raw transaction data before downstream processing."""
    cleaned = (
        df.withColumn("transaction_id", trim(col("transaction_id")))
        .withColumn("account_id", trim(col("account_id")))
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("transaction_time", col("timestamp").cast("timestamp"))
        .withColumn("merchant", trim(col("merchant")))
        .withColumn("transaction_type", trim(upper(col("transaction_type"))))
        .withColumn("amount", col("amount").cast("double"))
    )

    cleaned = cleaned.filter(
        col("transaction_id").isNotNull()
        & col("account_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("transaction_time").isNotNull()
        & col("amount").isNotNull()
        & (col("amount") > 0)
    )

    return cleaned.dropDuplicates(["transaction_id"])


def clean_customers(df: DataFrame) -> DataFrame:
    """Clean raw customer data."""
    cleaned = (
        df.withColumn("account_id", trim(col("account_id")))
        .withColumn("customer_name", trim(col("customer_name")))
        .withColumn("city", trim(col("city")))
        .withColumn("age", col("age").cast("int"))
    )

    return (
        cleaned.filter(
            col("account_id").isNotNull()
            & col("customer_name").isNotNull()
            & col("city").isNotNull()
            & col("age").isNotNull()
            & (col("age") > 0)
        )
        .dropDuplicates(["account_id"])
    )


def clean_merchants(df: DataFrame) -> DataFrame:
    """Clean raw merchant data."""
    cleaned = (
        df.withColumn("merchant_id", trim(col("merchant_id")))
        .withColumn("merchant_name", trim(col("merchant_name")))
        .withColumn("category", trim(col("category")))
    )

    return (
        cleaned.filter(
            col("merchant_id").isNotNull()
            & col("merchant_name").isNotNull()
            & col("category").isNotNull()
        )
        .dropDuplicates(["merchant_id"])
    )
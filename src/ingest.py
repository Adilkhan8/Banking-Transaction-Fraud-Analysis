from pathlib import Path
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession

from cleaning import clean_customers, clean_merchants, clean_transactions
from spark_session import get_spark_session


def _get_data_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "Data" / "raw"


def _read_csv(spark: SparkSession, filename: str) -> DataFrame:
    path = _get_data_dir() / filename
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .csv(str(path))
    )


def ingest_data() -> Tuple[DataFrame, DataFrame, DataFrame]:
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

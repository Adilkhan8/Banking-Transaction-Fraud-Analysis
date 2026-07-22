# Data ingestion module.
# Reads CSV files, applies cleaning rules, and returns the prepared datasets.
from pathlib import Path
from typing import Optional, Tuple, Union

from pyspark.sql import DataFrame, SparkSession

from cleaning import clean_customers, clean_merchants, clean_transactions
from spark_session import get_spark_session


def _get_default_data_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "Data" / "raw"


def _resolve_csv_path(path_value: Union[str, Path, None], data_dir: Optional[Path] = None) -> Path:
    base_dir = data_dir or _get_default_data_dir()

    if path_value is None:
        return base_dir

    candidate = Path(path_value)
    if candidate.is_absolute():
        return candidate

    return base_dir / candidate


def _read_csv(
    spark: SparkSession,
    filename: Union[str, Path, None],
    data_dir: Optional[Path] = None,
) -> DataFrame:
    path = _resolve_csv_path(filename, data_dir)
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .csv(str(path))
    )


def ingest_data(
    transactions_path: Optional[Union[str, Path]] = None,
    customers_path: Optional[Union[str, Path]] = None,
    merchants_path: Optional[Union[str, Path]] = None,
    data_dir: Optional[Path] = None,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Load and cleanse transaction, customer, and merchant CSV files."""
    spark = get_spark_session()

    transactions_raw = _read_csv(
        spark,
        transactions_path or "banking_transactions_20000.csv",
        data_dir,
    )
    customers_raw = _read_csv(
        spark,
        customers_path or "customers_1000.csv",
        data_dir,
    )
    merchants_raw = _read_csv(
        spark,
        merchants_path or "merchants.csv",
        data_dir,
    )

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

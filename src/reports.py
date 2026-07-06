# Reporting helpers for the fraud analysis workflow.
# These functions print summaries and sample results to the console.
from pyspark.sql import DataFrame


def print_dataset_summary(transactions: DataFrame, customers: DataFrame, merchants: DataFrame) -> None:
    """Print a concise summary of the cleaned datasets."""
    print("Loaded and cleaned datasets:")
    print("- transactions:", transactions.count())
    print("- customers:", customers.count())
    print("- merchants:", merchants.count())


def print_fraud_summary(flagged_transactions: DataFrame) -> None:
    """Print the fraud detection summary for the flagged results."""
    print("\nFraud detection results:")
    print("- flagged transactions:", flagged_transactions.count())
    print(
        "- distinct fraud reasons:",
        flagged_transactions.select("fraud_reason").where("fraud_reason IS NOT NULL").distinct().count(),
    )


def show_transactions(df: DataFrame, limit: int = 20) -> None:
    """Display the schema and a sample of the provided dataframe."""
    df.printSchema()
    df.show(limit, truncate=False)
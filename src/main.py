# Main entry point for the fraud analysis pipeline.
# Run this file to load data, clean it, detect fraud, and print results.
from io import StringIO
from pathlib import Path
from typing import Optional, Union

from fraud_rules import apply_all_fraud_rules
from ingest import ingest_data
from reports import print_dataset_summary, print_fraud_summary, show_transactions


def _capture_console_output(callback, *args, **kwargs) -> str:
    stream = StringIO()
    import sys

    original_stdout = sys.stdout
    sys.stdout = stream
    try:
        callback(*args, **kwargs)
    finally:
        sys.stdout = original_stdout
    return stream.getvalue()


def run_pipeline(
    show_sample: bool = True,
    sample_limit: int = 20,
    transactions_path: Optional[Union[str, Path]] = None,
    customers_path: Optional[Union[str, Path]] = None,
    merchants_path: Optional[Union[str, Path]] = None,
    capture_output: bool = False,
    return_results: bool = False,
) -> Optional[Union[str, dict]]:
    """Execute the full ingestion, cleaning, fraud detection, and reporting pipeline."""
    transactions, customers, merchants = ingest_data(
        transactions_path=transactions_path,
        customers_path=customers_path,
        merchants_path=merchants_path,
    )

    flagged_transactions = apply_all_fraud_rules(transactions)

    if capture_output:
        dataset_summary = _capture_console_output(print_dataset_summary, transactions, customers, merchants)
        fraud_summary = _capture_console_output(print_fraud_summary, flagged_transactions)
        return f"{dataset_summary}{fraud_summary}".strip()

    if return_results:
        return {
            "transactions": transactions,
            "customers": customers,
            "merchants": merchants,
            "flagged_transactions": flagged_transactions,
        }

    print_dataset_summary(transactions, customers, merchants)
    print_fraud_summary(flagged_transactions)

    if show_sample:
        show_transactions(flagged_transactions, sample_limit)

    return None


if __name__ == "__main__":
    run_pipeline()
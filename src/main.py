# Main entry point for the fraud analysis pipeline.
# Run this file to load data, clean it, detect fraud, and print results.
from fraud_rules import apply_all_fraud_rules
from ingest import ingest_data
from reports import print_dataset_summary, print_fraud_summary, show_transactions


def run_pipeline(show_sample: bool = True, sample_limit: int = 20) -> None:
    """Execute the full ingestion, cleaning, fraud detection, and reporting pipeline."""
    transactions, customers, merchants = ingest_data()

    print_dataset_summary(transactions, customers, merchants)

    flagged_transactions = apply_all_fraud_rules(transactions)
    print_fraud_summary(flagged_transactions)

    if show_sample:
        show_transactions(flagged_transactions, sample_limit)


if __name__ == "__main__":
    run_pipeline()
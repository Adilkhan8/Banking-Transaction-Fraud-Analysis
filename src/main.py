from ingest import ingest_data
from fraud_rules import apply_all_fraud_rules

def run_pipeline() -> None:
    """Execute the ingestion, cleansing, fraud detection, and reporting pipeline."""
    transactions, customers, merchants = ingest_data()

    print("Loaded and cleaned datasets:")
    print("- transactions:", transactions.count())
    print("- customers:", customers.count())
    print("- merchants:", merchants.count())

    flagged_transactions = apply_all_fraud_rules(transactions)

    print("\nFraud detection results:")
    print("- flagged transactions:", flagged_transactions.count())
    print("- distinct fraud reasons:", flagged_transactions.select("fraud_reason").distinct().count())

    flagged_transactions.printSchema()
    flagged_transactions.show(20, truncate=False)


if __name__ == "__main__":
    run_pipeline()
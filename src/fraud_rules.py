from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, lit, stddev, sum
from pyspark.sql.window import Window


def detect_high_value_transactions(df: DataFrame, threshold: int = 50000) -> DataFrame:
    """Flag transactions above the configured monetary threshold."""
    return (
        df.filter(col("amount") > threshold)
        .withColumn("fraud_flag", lit(True))
        .withColumn("fraud_reason", lit("High-value transaction"))
    )


def detect_multiple_transactions_short_period(
    df: DataFrame,
    time_window_minutes: int = 30,
    min_transactions: int = 3,
) -> DataFrame:
    """Flag customers who make several transactions inside a short window."""
    window_spec = (
        Window.partitionBy("customer_id")
        .orderBy(col("transaction_time"))
        .rangeBetween(-(time_window_minutes * 60), Window.currentRow)
    )

    df_with_count = df.withColumn("transaction_count_in_window", count("*").over(window_spec))

    return (
        df_with_count.filter(col("transaction_count_in_window") >= min_transactions)
        .withColumn("fraud_flag", lit(True))
        .withColumn(
            "fraud_reason",
            lit(f"Multiple transactions ({min_transactions}+) within {time_window_minutes} minutes"),
        )
    )


def detect_spending_spikes(df: DataFrame, spike_multiplier: float = 2.0) -> DataFrame:
    """Flag transactions that are much larger than a customer's average spend."""
    customer_avg_spending = df.groupBy("customer_id").agg(
        avg("amount").alias("avg_spending"),
        stddev("amount").alias("stddev_spending"),
    )

    df_with_stats = df.join(customer_avg_spending, "customer_id", "left")

    flagged = (
        df_with_stats.filter(col("amount") > (col("avg_spending") * spike_multiplier))
        .withColumn("fraud_flag", lit(True))
        .withColumn(
            "fraud_reason",
            lit(f"Spending spike - Amount {spike_multiplier}x above customer average"),
        )
    )

    return flagged.select(*df.columns, "fraud_flag", "fraud_reason", "avg_spending")


def detect_high_daily_transaction_frequency(
    df: DataFrame,
    max_transactions_per_day: int = 10,
) -> DataFrame:
    """Flag customers who exceed a daily transaction threshold."""
    df_with_date = df.withColumn("transaction_date", col("transaction_time").cast("date"))

    daily_counts = df_with_date.groupBy("customer_id", "transaction_date").agg(
        count("*").alias("daily_transaction_count"),
        sum("amount").alias("daily_total_amount"),
    )

    df_with_daily_counts = df_with_date.join(daily_counts, on=["customer_id", "transaction_date"], how="left")

    return (
        df_with_daily_counts.filter(col("daily_transaction_count") > max_transactions_per_day)
        .withColumn("fraud_flag", lit(True))
        .withColumn(
            "fraud_reason",
            lit(f"Excessive daily transactions - {max_transactions_per_day}+ transactions in a day"),
        )
    )


def apply_all_fraud_rules(
    df: DataFrame,
    high_value_threshold: int = 50000,
    time_window_minutes: int = 30,
    spike_multiplier: float = 2.0,
    max_daily_transactions: int = 10,
) -> DataFrame:
    """Apply all fraud detection rules and return the combined flagged transactions."""
    rule1_flagged = detect_high_value_transactions(df, high_value_threshold)
    rule2_flagged = detect_multiple_transactions_short_period(df, time_window_minutes)
    rule3_flagged = detect_spending_spikes(df, spike_multiplier)
    rule4_flagged = detect_high_daily_transaction_frequency(df, max_daily_transactions)

    flagged_transactions = (
        rule1_flagged.unionByName(rule2_flagged, allowMissingColumns=True)
        .unionByName(rule3_flagged, allowMissingColumns=True)
        .unionByName(rule4_flagged, allowMissingColumns=True)
    )

    return flagged_transactions.dropDuplicates()

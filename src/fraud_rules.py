from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum, avg, stddev, 
    lag, max, min, datediff, 
    window, row_number, desc
)
from pyspark.sql.window import Window


# Rule 1: High-Value Transactions
def detect_high_value_transactions(df: DataFrame, threshold: int = 50000) -> DataFrame:
    """
    Detect transactions with amount greater than the specified threshold (₹50,000).
    
    Args:
        df: Input DataFrame with transaction data
        threshold: Amount threshold in rupees (default: 50,000)
    
    Returns:
        DataFrame with flagged high-value transactions
    """
    return df.filter(col("amount") > threshold) \
        .withColumn("fraud_flag", col("amount") > threshold) \
        .withColumn("fraud_reason", col("High-value transaction")) \
        .select("*", col("fraud_flag"), col("fraud_reason"))


# Rule 2: Multiple Transactions in Short Period
def detect_multiple_transactions_short_period(df: DataFrame, time_window_minutes: int = 30, min_transactions: int = 3) -> DataFrame:
    """
    Detect customers making multiple transactions within a short period.
    
    Args:
        df: Input DataFrame with transaction data
        time_window_minutes: Time window in minutes (default: 30)
        min_transactions: Minimum transactions to flag (default: 3)
    
    Returns:
        DataFrame flagged for suspicious transaction frequency
    """
    # Window specification for counting transactions per customer in time period
    window_spec = Window.partitionBy("customer_id") \
        .orderBy(col("transaction_time").cast("timestamp")) \
        .rangeBetween(-time_window_minutes * 60, 0)
    
    df_with_count = df.withColumn(
        "transaction_count_in_window",
        count("*").over(window_spec)
    )
    
    flagged = df_with_count.filter(col("transaction_count_in_window") >= min_transactions) \
        .withColumn("fraud_flag", True) \
        .withColumn("fraud_reason", 
                   col(f"Multiple transactions ({min_transactions}+) within {time_window_minutes} minutes"))
    
    return flagged


# Rule 3: Spending Spike Detection
def detect_spending_spikes(df: DataFrame, spike_multiplier: float = 2.0) -> DataFrame:
    """
    Detect spending spikes - when a customer suddenly spends significantly more than their average.
    
    Args:
        df: Input DataFrame with transaction data
        spike_multiplier: Threshold multiplier (default: 2.0x average)
    
    Returns:
        DataFrame flagged for unusual spending patterns
    """
    # Calculate average spending per customer
    customer_avg_spending = df.groupBy("customer_id") \
        .agg(avg("amount").alias("avg_spending"),
             stddev("amount").alias("stddev_spending"))
    
    df_with_stats = df.join(customer_avg_spending, "customer_id")
    
    # Flag transactions where amount > avg_spending * multiplier
    flagged = df_with_stats.filter(
        col("amount") > (col("avg_spending") * spike_multiplier)
    ).withColumn("fraud_flag", True) \
     .withColumn("fraud_reason", 
                col(f"Spending spike - Amount {spike_multiplier}x above customer average"))
    
    return flagged.select(df.columns + ["fraud_flag", "fraud_reason", "avg_spending"])


# Rule 4: High Transaction Frequency in a Day
def detect_high_daily_transaction_frequency(df: DataFrame, max_transactions_per_day: int = 10) -> DataFrame:
    """
    Detect customers making too many transactions in a single day.
    
    Args:
        df: Input DataFrame with transaction data
        max_transactions_per_day: Maximum allowed transactions per day (default: 10)
    
    Returns:
        DataFrame flagged for excessive daily transaction frequency
    """
    # Extract date from transaction_time and count transactions per customer per day
    df_with_date = df.withColumn("transaction_date", 
                                 col("transaction_time").cast("date"))
    
    daily_counts = df_with_date.groupBy("customer_id", "transaction_date") \
        .agg(count("*").alias("daily_transaction_count"),
             sum("amount").alias("daily_total_amount"))
    
    # Rejoin with original data and flag high frequency days
    df_with_daily_counts = df_with_date.join(
        daily_counts,
        on=["customer_id", "transaction_date"],
        how="left"
    )
    
    flagged = df_with_daily_counts.filter(
        col("daily_transaction_count") > max_transactions_per_day
    ).withColumn("fraud_flag", True) \
     .withColumn("fraud_reason", 
                col(f"Excessive daily transactions - {max_transactions_per_day}+ transactions in a day"))
    
    return flagged


# Combine all rules - Flag transactions that meet any rule
def apply_all_fraud_rules(df: DataFrame, 
                         high_value_threshold: int = 50000,
                         time_window_minutes: int = 30,
                         spike_multiplier: float = 2.0,
                         max_daily_transactions: int = 10) -> DataFrame:
    """
    Apply all fraud detection rules to the transaction data.
    
    Args:
        df: Input DataFrame with transaction data
        high_value_threshold: Threshold for high-value transactions (default: 50,000)
        time_window_minutes: Time window for multiple transactions (default: 30)
        spike_multiplier: Spending spike multiplier (default: 2.0x)
        max_daily_transactions: Max transactions per day (default: 10)
    
    Returns:
        DataFrame with fraud flags and reasons for all detected anomalies
    """
    # Apply individual rules
    rule1_flagged = detect_high_value_transactions(df, high_value_threshold)
    rule2_flagged = detect_multiple_transactions_short_period(df, time_window_minutes)
    rule3_flagged = detect_spending_spikes(df, spike_multiplier)
    rule4_flagged = detect_high_daily_transaction_frequency(df, max_daily_transactions)
    
    # Combine all flagged transactions
    flagged_transactions = rule1_flagged.union(rule2_flagged).union(rule3_flagged).union(rule4_flagged)
    
    # Remove duplicates and return
    return flagged_transactions.dropDuplicates()

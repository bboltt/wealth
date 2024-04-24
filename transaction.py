from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_date, lit, expr, avg, when, add_months, substring

# Start Spark session
spark = SparkSession.builder.appName("Client Transaction Features Calculation").getOrCreate()

# Load data
df = spark.table("dm_wm.pwm_transactions")

# Convert ods_business_dt to date format
df = df.withColumn("date", to_date(col("ods_business_dt"), "yyyy-MM-dd"))

# Assume 'reference_df' has been loaded with 'ip_id' and 'open_date' for each client
reference_df = spark.table("your_reference_table").withColumn("open_date", to_date(col("open_date"), "yyyy-MM-dd"))

# Join transactions with the reference dates
df = df.join(reference_df, "ip_id", "inner")

# Define function to calculate features based on variable open_dates
def calculate_client_features(df, periods):
    # Prepare initial aggregations on transaction data
    client_aggregations = df.groupBy("ip_id", "open_date").agg(
        sum(col("transfer_outgoing_cnt") + col("mortgage_outgoing_cnt")).alias("trans_loan_n_mortgage_cnt"),
        sum(col("transfer_outgoing_amt") + col("mortgage_outgoing_amt")).alias("trans_loan_n_mortgage_amt")
    )

    # Initialize final results DataFrame from initial aggregations
    final_results = client_aggregations

    for months in periods:
        # Calculate the start date for the look-back period for each client based on their open_date
        period_data = df.withColumn("period_start_date", add_months(col("open_date"), -months))
        period_data = period_data.filter((col("date") > col("period_start_date")) & (col("date") <= col("open_date")))
        
        # Recalculate aggregates for each period
        period_aggregations = period_data.groupBy("ip_id", "open_date").agg(
            avg(col("transfer_outgoing_cnt") + col("mortgage_outgoing_cnt")).alias("avg_trans_loan_n_mortgage_cnt"),
            avg(col("transfer_outgoing_amt") + col("mortgage_outgoing_amt")).alias("avg_trans_loan_n_mortgage_amt")
        )
        
        # Join the period aggregations back to the final results DataFrame
        for metric in ["trans_loan_n_mortgage_cnt", "trans_loan_n_mortgage_amt"]:
            avg_metric = f"avg_{metric}_{months}m"
            final_results = final_results.join(period_aggregations.select("ip_id", "open_date", col(f"avg_{metric}").alias(avg_metric)),
                                               ["ip_id", "open_date"],
                                               "left")

            # Calculate difference and percentage difference
            final_results = final_results.withColumn(f"{metric}_diff_from_m{months}", 
                                                     col(metric) - col(avg_metric))
            final_results = final_results.withColumn(f"{metric}_sum_pct_diff_from_m{months}",
                                                     (col(metric) - col(avg_metric)) / when(col(avg_metric) != 0, col(avg_metric)).otherwise(1))

    return final_results

# Periods of interest for feature calculation
periods = [2, 3, 6, 12]

# Apply feature calculations
features_df = calculate_client_features(df, periods)

# Optionally, modify the client ID to only keep the last 8 characters and rename the column
features_df = features_df.withColumn("ip_id_short", substring(col("ip_id"), -8, 8))

# Show the result
features_df.show()



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_date, avg, add_months, when, substring

# Start Spark session
spark = SparkSession.builder.appName("Variable Date Transaction Features Calculation").getOrCreate()

# Load transaction data
transactions_df = spark.table("dm_wm.pwm_transactions")

# Load reference data with ip_id and variable open_dates
reference_df = spark.table("your_reference_table")  # Ensure this table is correctly referenced

# Convert date columns to date type
transactions_df = transactions_df.withColumn("date", to_date(col("ods_business_dt"), "yyyy-MM-dd"))
reference_df = reference_df.withColumn("open_date", to_date(col("open_date"), "yyyy-MM-dd"))

# Join transactions with reference to get individual reference dates for each ip_id
df = transactions_df.join(reference_df, "ip_id", "inner")

# Define function to calculate features based on individual open_dates
def calculate_client_features(df, periods):
    final_results = df.select("ip_id", "open_date").distinct()

    for months in periods:
        # Define the date range for each period based on the dynamic open_date
        df = df.withColumn("period_start_date", add_months(col("open_date"), -months))
        df = df.withColumn("period_end_date", col("open_date"))

        # Filter transactions within the date range for each period
        period_data = df.filter((col("date") > col("period_start_date")) & (col("date") <= col("period_end_date")))
        
        # Aggregate data for this period by ip_id
        period_aggregations = period_data.groupBy("ip_id", "open_date").agg(
            avg(col("transfer_outgoing_cnt") + col("mortgage_outgoing_cnt")).alias("avg_trans_loan_n_mortgage_cnt"),
            avg(col("transfer_outgoing_amt") + col("mortgage_outgoing_amt")).alias("avg_trans_loan_n_mortgage_amt")
        )
        
        # Join period aggregations back to the results DataFrame
        for metric in ["trans_loan_n_mortgage_cnt", "trans_loan_n_mortgage_amt"]:
            avg_metric = f"avg_{metric}_{months}m"
            final_results = final_results.join(period_aggregations.select("ip_id", "open_date", col(f"avg_trans_loan_n_mortgage_cnt").alias(avg_metric)),
                                               ["ip_id", "open_date"],
                                               "left")

            # Calculate difference and percentage difference
            final_results = final_results.withColumn(f"{metric}_diff_from_m{months}", 
                                                     col(metric) - col(avg_metric))
            final_results = final_results.withColumn(f"{metric}_sum_pct_diff_from_m{months}",
                                                     (col(metric) - col(avg_metric)) / when(col(avg_metric) != 0, col(avg_metric)).otherwise(1))

    return final_results

# Periods of interest for feature calculation
periods = [2, 3, 6]

# Apply feature calculations
features_df = calculate_client_features(df, periods)

# Optionally, modify the client ID to only keep the last 8 characters and rename the column
features_df = features_df.withColumn("ip_id_short", substring(col("ip_id"), -8, 8))

# Show the result
features_df.show()



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, expr, avg, add_months, when, substring

# Start Spark session
spark = SparkSession.builder.appName("Client Transaction Features Calculation").getOrCreate()

# Load data
transactions_df = spark.table("dm_wm.pwm_transactions")
reference_df = spark.table("your_reference_table")  # Load your reference DataFrame

# Convert date columns to date type
transactions_df = transactions_df.withColumn("date", to_date(col("ods_business_dt"), "yyyy-MM-dd"))
reference_df = reference_df.withColumn("open_date", to_date(col("open_date"), "yyyy-MM-dd"))

# Join reference_df to transactions_df to get the individual reference dates
df = transactions_df.join(reference_df, "ip_id", "inner")

# Define function to calculate features based on individual reference dates
def calculate_client_features(df, periods):
    final_results = df.select("ip_id", "open_date").distinct()

    for months in periods:
        # Calculate the start date of the period based on each open_date
        period_data = df.withColumn("period_start_date", add_months(col("open_date"), -months))
        
        # Filter transactions for the specific period
        period_data = period_data.filter((col("date") > col("period_start_date")) & (col("date") <= col("open_date")))
        
        # Aggregate data for this period
        period_aggregations = period_data.groupBy("ip_id", "open_date").agg(
            avg("curr_bal_amt").alias(f"avg_bal_{months}m_hh_id")
        )
        
        # Join the period aggregations back to the final results DataFrame
        final_results = final_results.join(period_aggregations, ["ip_id", "open_date"], "left")

    return final_results

# Periods of interest
periods = [1, 3, 6]

# Apply feature calculations
features_df = calculate_client_features(df, periods)

# Modify the client ID to only keep the last 8 characters and rename the column
features_df = features_df.withColumn("ip_id", substring(col("ip_id"), -8, 8))

# Show the result
features_df.show()


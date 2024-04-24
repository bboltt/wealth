from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, expr, avg, when, add_months

# Start Spark session
spark = SparkSession.builder.appName("Household Balance Features Calculation").getOrCreate()

# Load data
df = spark.table("dm_r3.pwm_mstr_dtl_daily")

# Convert date columns to date type
df = df.withColumn("business_date", to_date(col("business_date"), "yyyy-MM-dd"))
df = df.withColumn("open_date", to_date(col("open_date"), "yyyy-MM-dd"))
df = df.withColumn("close_date", to_date(col("close_date"), "yyyy-MM-dd"))

# Define the reference date
reference_date = to_date(lit("2024-01-01"), "yyyy-MM-dd")

# Filter data up to and including the reference date
df = df.filter(col("business_date") <= reference_date)

# Include transactions from up to 6 months before the reference date
look_back_date = add_months(reference_date, -6)
df = df.filter(col("business_date") > look_back_date)

def calculate_hh_features(df, reference_date, periods):
    results = df.select("ip_id", "hh_id_in_wh").distinct()

    for months in periods:
        # Define the date range for each period
        period_start_date = add_months(reference_date, -months)
        
        # Filter the main DataFrame for the period
        period_data = df.filter((col("business_date") > period_start_date) & (col("business_date") <= reference_date))
        
        # Aggregate data for this period
        period_aggregations = period_data.groupBy("hh_id_in_wh").agg(
            avg("curr_bal_amt").alias(f"avg_bal_{months}m_hh_id")
        )
        
        # Join the period aggregations back to the results DataFrame
        results = results.join(period_aggregations, "hh_id_in_wh", "left")

    return results

# Periods of interest
periods = [1, 3, 6]

# Apply feature calculations
features_df = calculate_hh_features(df, reference_date, periods)

# Show the result
features_df.show()

# You might also consider dropping duplicate 'ip_id' if any exists, ensuring unique rows per 'ip_id'
features_df = features_df.dropDuplicates(["ip_id"])

# Show the final DataFrame
features_df.show()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, expr, avg, when, add_months

# Start Spark session
spark = SparkSession.builder.appName("Household Balance Features Calculation").getOrCreate()

# Load data
df = spark.table("dm_r3.pwm_mstr_dtl_daily")

# Assume 'reference_df' has been loaded with 'ip_id' and variable 'open_dates'
reference_df = spark.table("your_reference_table").withColumn("open_date", to_date(col("open_date"), "yyyy-MM-dd"))

# Convert date columns to date type
df = df.withColumn("business_date", to_date(col("business_date"), "yyyy-MM-dd"))
df = df.withColumn("close_date", to_date(col("close_date"), "yyyy-MM-dd"))

# Join data with the reference dates
df = df.join(reference_df, "ip_id", "inner")

def calculate_hh_features(df, periods):
    # Initialize the results DataFrame from the joined data
    results = df.select("ip_id", "hh_id_in_wh", "open_date").distinct()

    for months in periods:
        # Adjust the data frame to include a dynamic period start date for each reference date
        period_data = df.withColumn("period_start_date", add_months(col("open_date"), -months))
        period_data = period_data.filter((col("business_date") > col("period_start_date")) & (col("business_date") <= col("open_date")))
        
        # Aggregate balance data for each household over this period
        period_aggregations = period_data.groupBy("hh_id_in_wh").agg(
            avg("curr_bal_amt").alias(f"avg_bal_{months}m_hh_id")
        )
        
        # Join the period aggregations back to the results DataFrame using the household ID
        results = results.join(period_aggregations, "hh_id_in_wh", "left")

    return results

# Periods of interest for balance calculation
periods = [1, 3, 6]

# Apply feature calculations
features_df = calculate_hh_features(df, periods)

# Show the intermediate results
features_df.show()

# Optionally, drop duplicate 'ip_id' entries to ensure unique rows per 'ip_id'
features_df = features_df.dropDuplicates(["ip_id"])

# Show the final DataFrame
features_df.show()


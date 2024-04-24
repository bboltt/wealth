from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, expr, last_day, to_date, format_string, date_format, lit
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Start Spark session
spark = SparkSession.builder.appName("Client Transaction Features Calculation").getOrCreate()

# Load data
df = spark.table("dm_wm.pwm_transactions")

# Convert ods_business_dt to date format
df = df.withColumn("date", to_date(col("ods_business_dt"), "yyyy-MM-dd"))

# Filter data up to a specific reference date (e.g., "2024-01-01")
reference_date = to_date(lit("2024-01-01"), "yyyy-MM-dd")
df = df.filter(col("date") <= reference_date)

# Define the maximum look-back period based on the longest period needed for feature calculation
max_look_back_months = 12  # As 'm12' is the longest period we are calculating
look_back_date = reference_date - expr(f"INTERVAL {max_look_back_months} MONTHS")
df = df.filter(col("date") > look_back_date)

# Group by client ID and calculate sums for relevant fields up to the reference date
client_aggregations = df.groupBy("unq_id_in_src_sys").agg(
    sum(col("transfer_outgoing_cnt") + col("mortgage_outgoing_cnt")).alias("trans_loan_n_mortgage_cnt"),
    sum(col("transfer_outgoing_amt") + col("mortgage_outgoing_amt")).alias("trans_loan_n_mortgage_amt")
)

# Calculate features as differences from the reference month values
def calculate_client_features(df, metric):
    # Compute differences and percentage differences
    for months in [2, 3, 6, 12]:
        # Define the end of the period to compute the averages
        period_end_date = reference_date - expr(f"INTERVAL {months} MONTHS")
        period_start_date = period_end_date - expr(f"INTERVAL {months} MONTHS")

        # Filter data to the specific period
        period_data = df.filter((col("date") > period_start_date) & (col("date") <= period_end_date))
        
        # Compute the average for the metric over this period
        avg_metric = period_data.groupBy("unq_id_in_src_sys").avg(metric).alias(f"{metric}_avg_{months}m")

        # Join back to the main dataset to compute differences
        df = df.join(avg_metric, "unq_id_in_src_sys", "left")
        df = df.withColumn(f"{metric}_diff_from_m{months}", col(metric) - col(f"{metric}_avg_{months}m"))
        df = df.withColumn(f"{metric}_sum_pct_diff_from_m{months}",
                           (col(metric) - col(f"{metric}_avg_{months}m")) / when(col(f"{metric}_avg_{months}m") != 0, col(f"{metric}_avg_{months}m")).otherwise(1))

    return df

# Compute features for counts and amounts for each client
features_df = client_aggregations
for field in ["trans_loan_n_mortgage_cnt", "trans_loan_n_mortgage_amt"]:
    features_df = calculate_client_features(features_df, field)


# Display the result
features_df.show()


# To get a DataFrame where each row corresponds to a client and each column is a feature, pivot the DataFrame
# This step might require additional memory depending on the size of your data
final_features_df = features_df.groupBy("unq_id_in_src_sys").pivot("year_month").agg(
    *[lit(x).alias(x) for x in features_df.columns if x not in ["unq_id_in_src_sys", "year_month"]]
)

final_features_df.show(truncate=False)

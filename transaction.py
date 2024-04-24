from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_date, date_format, expr, when, avg
from pyspark.sql.window import Window

# Start Spark session
spark = SparkSession.builder.appName("Client Transaction Features Calculation").getOrCreate()

# Load data
df = spark.table("dm_wm.pwm_transactions")

# Convert ods_business_dt to date format
df = df.withColumn("date", to_date(col("ods_business_dt"), "yyyy-MM-dd"))

# Define the reference date
reference_date = to_date(lit("2024-01-01"), "yyyy-MM-dd")

# Filter data up to and including the reference date
df = df.filter(col("date") <= reference_date)

# Include transactions from up to 12 months before the reference date
max_look_back_months = 12
look_back_date = expr("add_months(to_date('2024-01-01'), -12)")
df = df.filter(col("date") > look_back_date)

# Group by client ID and aggregate sums for relevant fields
client_aggregations = df.groupBy("unq_id_in_src_sys").agg(
    sum(col("transfer_outgoing_cnt") + col("mortgage_outgoing_cnt")).alias("trans_loan_n_mortgage_cnt"),
    sum(col("transfer_outgoing_amt") + col("mortgage_outgoing_amt")).alias("trans_loan_n_mortgage_amt")
).cache()  # Cache for performance improvement if subsequent operations are extensive

# Calculate features as differences from the reference month values
def calculate_client_features(df, reference_df, metric, periods):
    results = reference_df.select("unq_id_in_src_sys", metric)

    for months in periods:
        # Filter for the specific period
        period_start_date = expr(f"add_months('{reference_date}', -{months})")
        period_end_date = reference_date
        
        period_data = df.filter((col("date") > period_start_date) & (col("date") <= period_end_date))
        period_aggregations = period_data.groupBy("unq_id_in_src_sys").agg(
            avg(metric).alias(f"{metric}_avg_{months}m")
        )

        # Join period aggregations with the main DataFrame
        results = results.join(period_aggregations, "unq_id_in_src_sys", "left")

        # Calculate the difference and percentage difference
        results = results.withColumn(f"{metric}_diff_from_m{months}", col(metric) - col(f"{metric}_avg_{months}m"))
        results = results.withColumn(f"{metric}_sum_pct_diff_from_m{months}",
                                     (col(metric) - col(f"{metric}_avg_{months}m")) / when(col(f"{metric}_avg_{months}m") != 0, col(f"{metric}_avg_{months}m")).otherwise(1))

    return results

# Compute features for each client
features_df = client_aggregations
for field in ["trans_loan_n_mortgage_cnt", "trans_loan_n_mortgage_amt"]:
    features_df = calculate_client_features(df, features_df, field, [2, 3, 6, 12])

# Display the result
features_df.show()



# To get a DataFrame where each row corresponds to a client and each column is a feature, pivot the DataFrame
# This step might require additional memory depending on the size of your data
final_features_df = features_df.groupBy("unq_id_in_src_sys").pivot("year_month").agg(
    *[lit(x).alias(x) for x in features_df.columns if x not in ["unq_id_in_src_sys", "year_month"]]
)

final_features_df.show(truncate=False)

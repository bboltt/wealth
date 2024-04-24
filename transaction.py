from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, expr, last_day, to_date, format_string, date_format, lit
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Start Spark session
spark = SparkSession.builder.appName("Client Transaction Features Calculation").getOrCreate()

# Load data
df = spark.table("dm_wm.pwm_transactions")

# Convert ods_business_dt to date format and extract year-month
df = df.withColumn("date", to_date(col("ods_business_dt"), "yyyy-MM-dd"))
df = df.withColumn("year_month", date_format("date", "yyyy-MM"))

# Group by client ID and year_month, and aggregate sums for relevant fields
client_monthly_aggregations = df.groupBy("unq_id_in_src_sys", "year_month").agg(
    sum(col("transfer_outgoing_cnt") + col("mortgage_outgoing_cnt")).alias("trans_loan_n_mortgage_cnt"),
    sum(col("transfer_outgoing_amt") + col("mortgage_outgoing_amt")).alias("trans_loan_n_mortgage_amt")
)

# Define a function to compute the features for each client
def calculate_client_features(df, metric, periods):
    # Window specification partitioned by client and ordered by month
    windowSpec = Window.partitionBy("unq_id_in_src_sys").orderBy("year_month")
    
    for months in periods:
        # Rolling sum for previous months within the same client
        rolling_sum = F.sum(metric).over(windowSpec.rangeBetween(-months, -1))
        
        # Calculate difference and percentage difference
        df = df.withColumn(f"{metric}_diff_from_m{months}", col(metric) - (rolling_sum / months))
        df = df.withColumn(f"{metric}_sum_pct_diff_from_m{months}",
                           (col(metric) - (rolling_sum / months)) / (rolling_sum / months))
    return df

# Periods of interest
periods = [2, 3, 6, 12]

# Compute features for counts and amounts for each client
features_df = client_monthly_aggregations
for field in ["trans_loan_n_mortgage_cnt", "trans_loan_n_mortgage_amt"]:
    features_df = calculate_client_features(features_df, field, periods)

# Display the result
features_df.show()

# To get a DataFrame where each row corresponds to a client and each column is a feature, pivot the DataFrame
# This step might require additional memory depending on the size of your data
final_features_df = features_df.groupBy("unq_id_in_src_sys").pivot("year_month").agg(
    *[lit(x).alias(x) for x in features_df.columns if x not in ["unq_id_in_src_sys", "year_month"]]
)

final_features_df.show(truncate=False)

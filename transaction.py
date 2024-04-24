from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_date, lit, expr, avg, when, add_months
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
look_back_date = add_months(reference_date, -12)
df = df.filter(col("date") > look_back_date)

# Group by client ID and aggregate sums for relevant fields
client_aggregations = df.groupBy("unq_id_in_src_sys").agg(
    sum(col("transfer_outgoing_cnt") + col("mortgage_outgoing_cnt")).alias("trans_loan_n_mortgage_cnt"),
    sum(col("transfer_outgoing_amt") + col("mortgage_outgoing_amt")).alias("trans_loan_n_mortgage_amt")
)

def calculate_client_features(df, reference_df, periods):
    final_results = reference_df

    for months in periods:
        # Define the date range for each period
        period_start_date = add_months(reference_date, -months)
        
        # Filter the main DataFrame for the period
        period_data = df.filter((col("date") > period_start_date) & (col("date") <= reference_date))
        
        # Aggregate data for this period
        period_aggregations = period_data.groupBy("unq_id_in_src_sys").agg(
            avg(col("transfer_outgoing_cnt") + col("mortgage_outgoing_cnt")).alias("avg_trans_loan_n_mortgage_cnt"),
            avg(col("transfer_outgoing_amt") + col("mortgage_outgoing_amt")).alias("avg_trans_loan_n_mortgage_amt")
        )
        
        # Join the period aggregations back to the final results DataFrame
        for metric in ["trans_loan_n_mortgage_cnt", "trans_loan_n_mortgage_amt"]:
            avg_metric = f"avg_{metric}_{months}m"
            final_results = final_results.join(period_aggregations.select("unq_id_in_src_sys", col("avg_trans_loan_n_mortgage_cnt").alias(avg_metric)),
                                               "unq_id_in_src_sys",
                                               "left")
            
            # Calculate the difference and percentage difference
            final_results = final_results.withColumn(f"{metric}_diff_from_m{months}", 
                                                     col(metric) - col(avg_metric))
            final_results = final_results.withColumn(f"{metric}_sum_pct_diff_from_m{months}",
                                                     (col(metric) - col(avg_metric)) / when(col(avg_metric) != 0, col(avg_metric)).otherwise(1))

    return final_results

# Apply feature calculations
features_df = calculate_client_features(df, client_aggregations, [2, 3, 6, 12])

# Show the result
features_df.show()






# To get a DataFrame where each row corresponds to a client and each column is a feature, pivot the DataFrame
# This step might require additional memory depending on the size of your data
final_features_df = features_df.groupBy("unq_id_in_src_sys").pivot("year_month").agg(
    *[lit(x).alias(x) for x in features_df.columns if x not in ["unq_id_in_src_sys", "year_month"]]
)

final_features_df.show(truncate=False)

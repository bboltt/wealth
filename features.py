from pyspark.sql.functions import col, max as sql_max, min as sql_min, avg as sql_avg, sum as sql_sum, date_sub, current_date
from pyspark.sql.window import Window

class FeatureEngineeringPipeline:
    def __init__(self, df, reference_date, doc_path):
        self.df = df
        self.reference_date = reference_date
        self.doc_path = doc_path
        self.hh_features = None

    def execute(self, spark):
        self.add_revenue_features()
        # Add other feature engineering steps here
        # e.g., self.add_balance_features()

    def add_revenue_features(self):
        """
        Adds revenue-related features, including current revenue, revenue over 3 months, 6 months, 
        and revenue max/min over the periods.
        """
        # Filter revenue data
        revenue_df = self.df.select("hh_id_in_wh", "revenue_amt", "transaction_date")

        # Define periods
        three_months_ago = date_sub(current_date(), 90)
        six_months_ago = date_sub(current_date(), 180)

        # Calculate revenue features
        revenue_features = revenue_df.groupBy("hh_id_in_wh").agg(
            sql_sum("revenue_amt").alias("revenue_cur"),
            sql_avg(F.when(col("transaction_date") >= three_months_ago, col("revenue_amt"))).alias("revenue_3months"),
            sql_avg(F.when(col("transaction_date") >= six_months_ago, col("revenue_amt"))).alias("revenue_6months"),
            sql_max("revenue_amt").alias("revenue_max"),
            sql_min("revenue_amt").alias("revenue_min")
         . 

---

We can now add Logic

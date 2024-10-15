from pyspark.sql.functions import broadcast, col, lit, udf, array, least, avg
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
from private_wealth_retention.consumer_prospecting.feature_engineering_pipeline import normalize_features
from private_wealth_retention.consumer_prospecting.model_pipeline import calculate_similarity, perform_clustering

def get_prospects(spark, pwm_df, consumer_df, k, n, feature_cols):
    """
    Generates prospects based on the similarity between consumer data and PWM clusters.
    
    Args:
        spark: Spark session.
        pwm_df: DataFrame of PWM clients (affluent or high net worth).
        consumer_df: DataFrame of consumer prospects.
        k: Number of clusters.
        n: Number of top prospects to return.
        feature_cols: List of feature columns to use in clustering and similarity calculation.
    
    Returns:
        DataFrame: Prospects with similarity score, cluster statistics, and individual values.
    """
    # Perform clustering on PWM data
    cluster_centers, assembler, scaler = perform_clustering(spark, pwm_df, feature_cols, k)
    
    # Add individual consumer feature vectors
    consumer_df = assembler.transform(consumer_df)
    consumer_df = scaler.transform(consumer_df)

    # Calculate similarity between consumers and cluster centers
    similarity_df = calculate_similarity(spark, consumer_df, cluster_centers, assembler, scaler)

    # Calculate mean values for each cluster for specific columns
    cluster_stats = pwm_df.groupBy("cluster_id").agg(
        avg("curr_bal_amt").alias("cluster_avg_curr_bal_amt"),
        avg("balance_trend").alias("cluster_avg_balance_trend"),
        avg("product_diversity").alias("cluster_avg_product_diversity")
    )

    # Join similarity results with consumer's individual values and cluster stats
    final_df = similarity_df \
        .join(consumer_df.select("hh_id_in_wh", "curr_bal_amt", "balance_trend", "product_diversity"), "hh_id_in_wh", "left") \
        .join(cluster_stats, "cluster_id", "left") \
        .select(
            col("hh_id_in_wh"),
            col("min_similarity").alias("similarity_score"),
            col("cluster_avg_curr_bal_amt"),
            col("cluster_avg_balance_trend"),
            col("cluster_avg_product_diversity"),
            col("curr_bal_amt"),
            col("balance_trend"),
            col("product_diversity")
        )

    return final_df.orderBy(F.col("similarity_score").desc()).limit(n)

from HiveIO.IO import read_hive_table
from private_wealth_retention.consumer_prospecting.model_pipeline import calculate_similarity, perform_clustering
import pyspark.sql.functions as F

def get_prospects(spark, group, k, n, feature_cols):
    # Load pre-engineered features from the Hive table
    pwm_features = read_hive_table(spark, "SB_DSP.prospecting_pwm_features_test")
    
    # Filter based on the group (affluent or high net worth)
    pwm_features = pwm_features.filter(F.col("segmt_label") == group)
    
    # Perform clustering
    cluster_centers, assembler, scaler = perform_clustering(spark, pwm_features, feature_cols, k)
    
    # Load consumers data for generating prospects
    consumer_features = pwm_features.filter(F.col("pwm_label") == "consumer")
    
    # Calculate similarity between consumer features and cluster centers
    similarity_df = calculate_similarity(spark, consumer_features, cluster_centers, assembler, scaler)
    
    # Return top N prospects based on similarity score
    top_n_prospects = similarity_df.orderBy(F.col("min_similarity")).limit(n)
    return top_n_prospects

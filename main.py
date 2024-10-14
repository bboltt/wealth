from optimal_spark_config.create_spark_instance import generate_spark_instance
from HiveIO.IO import read_hive_table
from pyspark.sql import Row
from private_wealth_retention.consumer_prospecting.model_pipeline import calculate_similarity, perform_clustering
from private_wealth_retention.consumer_prospecting.prospecting import get_prospects
from HiveIO.config import get_config
import private_wealth_retention.consumer_prospecting as cp

if __name__ == "__main__":
    # Load config and initialize Spark
    spark = generate_spark_instance(total_memory=600, total_vcpu=300)
    cfg = get_config(cp, "config.yaml")
    features = list(cfg["features"]["feature_names"])
    
    # Load pre-engineered features from Hive (skip the feature engineering pipeline)
    pwm_features = read_hive_table(spark, "SB_DSP.prospecting_pwm_features_test")
    
    k, n = 10, 1000
    
    # Perform clustering on precomputed features
    affluent_prospects = get_prospects(spark, "affluent", k, n, features)
    hnw_prospects = get_prospects(spark, "high net worth", k, n, features)
    
    # Further steps like validating the prospects, saving results, etc., would go here
    # e.g., pwm_prospects["affluent"] = affluent_prospects, etc.

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.sql import Row
import numpy as np
from pyspark.sql.functions import col, lit, array, least
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType

# Perform clustering on pre-engineered data
def perform_clustering(spark, df, features, k):
    """
    Performs K-Means clustering on pre-engineered PWM data and returns the cluster centers.
    
    Args:
        df (DataFrame): Pre-engineered DataFrame containing PWM features.
        features (list): List of feature names to include in the clustering.
        k (int): Number of clusters.
        
    Returns:
        DataFrame: DataFrame containing cluster centers.
    """
    assembler = VectorAssembler(inputCols=features, outputCol="features", handleInvalid="skip")
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    
    kmeans = KMeans(featuresCol="scaledFeatures", k=k, seed=42)
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    
    model = pipeline.fit(df)
    centers = model.stages[-1].clusterCenters()
    
    centers_rows = [Row(features=center.tolist()) for center in centers]
    centers_df = spark.createDataFrame(centers_rows)
    return centers_df, assembler, model.stages[1]

# Calculate cosine similarity based on the pre-engineered feature data
def calculate_similarity(spark, df, cluster_centers, assembler, scaler):
    df = assembler.transform(df)
    df = scaler.transform(df)
    
    cluster_centers.cache()
    for i, center in enumerate(cluster_centers.collect()):
        center_features = center["features"]
        df = df.withColumn(f"similarity_to_cluster_{i}",
                          cosine_similarity_udf(col("scaledFeatures"), array([lit(x) for x in center_features])))
        
    similarity_columns = [f"similarity_to_cluster_{i}" for i, _ in enumerate(cluster_centers.collect())]
    df = df.withColumn("min_similarity", least(*[col(c) for c in similarity_columns]))
    return df

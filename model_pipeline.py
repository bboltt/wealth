from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.sql import Row
from pyspark.sql.functions import monotonically_increasing_id



def perform_clustering(spark, df, features, k):
    """
    Performs K-Means clustering on the PWM data and returns the cluster centers and 
    the DataFrame with assigned cluster_ids.
    
    Args:
        df (DataFrame): DataFrame containing only PWM client data.
        features (list): List of feature names to include in the clustering.
        k (int): Number of clusters.
        
    Returns:
        Tuple[DataFrame, DataFrame, VectorAssembler, StandardScaler]: The updated DataFrame with `cluster_id` added,
        cluster centers, the assembler, and the scaler.
    """
    assembler = VectorAssembler(inputCols=features, outputCol="features", handleInvalid="skip")
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")

    # Pipeline: Assemble features -> Normalize -> Cluster
    kmeans = KMeans(featuresCol="scaledFeatures", k=k, seed=42)
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    
    model = pipeline.fit(df)
    
    # Assign cluster IDs to each PWM client
    df_with_clusters = model.transform(df).withColumn("cluster_id", F.col("prediction"))

    # Retrieve cluster centers
    centers = model.stages[-1].clusterCenters()
    centers_rows = [Row(features=center.tolist(), cluster_id=i) for i, center in enumerate(centers)]
    centers_df = spark.createDataFrame(centers_rows)

    return df_with_clusters, centers_df, assembler, model.stages[1]


def calculate_similarity(spark, df, cluster_centers, assembler, scaler):
    """
    Calculates similarity between feature vectors and cluster centers using cosine similarity.
    
    Args:
        spark (SparkSession): Spark session.
        df (DataFrame): DataFrame containing features of consumer prospects.
        cluster_centers (DataFrame): DataFrame containing cluster centers.
        assembler: VectorAssembler used for assembling features.
        scaler: StandardScaler used for scaling features.
    
    Returns:
        DataFrame: DataFrame with similarity score and associated cluster.
    """
    df = assembler.transform(df)
    df = scaler.transform(df)

    cluster_centers.cache()
    for i, center in enumerate(cluster_centers.collect()):
        center_features = center["features"]
        df = df.withColumn(f"similarity_to_cluster_{i}",
                           cosine_similarity_udf(col("scaledFeatures"), array([lit(x) for x in center_features])))

    similarity_columns = [f"similarity_to_cluster_{i}" for i, _ in enumerate(cluster_centers.collect())]
    
    # Find the most similar cluster and its corresponding similarity score
    df = df.withColumn("min_similarity", least(*[col(c) for c in similarity_columns]))

    # Identify the closest cluster ID
    df = df.withColumn("cluster_id", F.expr(f"array_position(array({','.join(similarity_columns)}), min_similarity) - 1"))
    
    return df


































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

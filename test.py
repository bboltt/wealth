from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F

def time_series_split_spark(df, date_col, test_size):
    # Convert business_date to date type if not already
    df = df.withColumn(date_col, F.to_date(col(date_col)))
    # Sorting by date to split
    df = df.orderBy(col(date_col))
    total_rows = df.count()
    split_point = int(total_rows * (1 - test_size))
    train_df = df.limit(split_point)
    test_df = df.subtract(train_df)
    return train_df, test_df

def subsample_stratified_spark(df, label_col, product_col, zero_ratio, at_least, k, initial_seed):
    import numpy as np

    # Generate k random seeds based on the initial seed
    np.random.seed(initial_seed)
    seeds = np.random.randint(0, 10000, size=k)
    
    subsampled_data_dict = {}
    
    for seed in seeds:
        # Filter data by label
        df_flag_0 = df.filter(F.col(label_col) == 0)
        df_flag_1 = df.filter(F.col(label_col) == 1)
        
        # Count by product type
        df_group_flag_0 = df_flag_0.groupBy(product_col).count()
        df_group_flag_1 = df_flag_1.groupBy(product_col).count()
        
        # Adjust counts based on specified ratio and at_least parameter
        df_group_flag_1 = df_group_flag_1.withColumn('adjusted_count', F.col('count') * zero_ratio)
        join_expr = df_group_flag_1[product_col] == df_group_flag_0[product_col]
        df_group_flag = df_group_flag_1.join(df_group_flag_0, join_expr, 'outer')

        # Handle product types with no Flag=1 records
        df_group_flag = df_group_flag.na.fill({'count': at_least / zero_ratio, 'adjusted_count': at_least})

        # Calculate fraction for subsampling
        df_group_flag = df_group_flag.withColumn('fraction_col', F.col('adjusted_count') / F.col('count'))
        fraction_dict = df_group_flag.select(product_col, 'fraction_col').rdd.collectAsMap()
        
        # Apply subsampling to Flag=0 data
        sample_data_0 = df_flag_0.sampleBy(product_col, fractions=fraction_dict, seed=int(seed))
        sample_data = sample_data_0.union(df_flag_1)
        
        subsampled_data_dict[seed] = sample_data
    
    return subsampled_data_dict

# Example usage
spark = SparkSession.builder.appName("DataProcessing").getOrCreate()
data = spark.sql("SELECT * FROM sb_dsp.recommendation_train_data_v2")
train_df, test_df = time_series_split_spark(data, 'business_date', 0.2)
subsampled_train = subsample_stratified_spark(train_df, 'Flag', 'Segmt_Prod_Type', 2, 10, 5, 1234)

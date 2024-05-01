from pyspark.sql.functions import col

def subsample_stratified_spark(df, label_col, product_col, zero_ratio, at_least, k, initial_seed):
    import numpy as np

    # Generate k random seeds based on the initial seed
    np.random.seed(initial_seed)
    seeds = np.random.randint(0, 10000, size=k)
    
    subsampled_data_dict = {}
    
    for seed in seeds:
        # Filter data by label and ensure no null values in the product column
        df = df.filter(col(product_col).isNotNull())
        df_flag_0 = df.filter(col(label_col) == 0)
        df_flag_1 = df.filter(col(label_col) == 1)
        
        # Count by product type and rename columns to avoid ambiguity
        df_group_flag_0 = df_flag_0.groupBy(product_col).count().withColumnRenamed('count', 'count_0')
        df_group_flag_1 = df_flag_1.groupBy(product_col).count().withColumnRenamed('count', 'count_1')
        
        # Join DataFrames with clear distinction of product columns
        df_group_flag = df_group_flag_1.join(df_group_flag_0, df_group_flag_1[product_col] == df_group_flag_0[product_col], 'outer')
        
        # After join, handle missing counts
        df_group_flag = df_group_flag.na.fill({
            'count_0': at_least / zero_ratio,
            'count_1': 0,  # Assuming no 'count_1' leads to a zero value
            'adjusted_count': 0
        }).withColumn('adjusted_count', col('count_1') * zero_ratio)
        
        # Calculate fraction for subsampling
        df_group_flag = df_group_flag.withColumn('fraction_col', col('adjusted_count') / col('count_0'))
        fraction_dict = df_group_flag.select(product_col, 'fraction_col').na.fill(0).rdd.collectAsMap()
        
        # Check and debug fraction_dict for None or unexpected values
        if None in fraction_dict:
            print("None found in fraction dictionary, removing entry")
            del fraction_dict[None]
        
        # Apply subsampling to Flag=0 data
        sample_data_0 = df_flag_0.sampleBy(product_col, fractions=fraction_dict, seed=int(seed))
        sample_data = sample_data_0.union(df_flag_1)
        
        subsampled_data_dict[seed] = sample_data
    
    return subsampled_data_dict





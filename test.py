from pyspark.sql.functions import col

def subsample_stratified_spark(df, label_col, product_col, zero_ratio, at_least, k, initial_seed):
    import numpy as np

    # Generate k random seeds based on the initial seed
    np.random.seed(initial_seed)
    seeds = np.random.randint(0, 10000, size=k)
    
    subsampled_data_dict = {}
    
    for seed in seeds:
        # Filter data by label
        df_flag_0 = df.filter(col(label_col) == 0)
        df_flag_1 = df.filter(col(label_col) == 1)
        
        # Count by product type and rename columns to avoid ambiguity
        df_group_flag_0 = df_flag_0.groupBy(product_col).count().withColumnRenamed('count', 'count_0')
        df_group_flag_1 = df_flag_1.groupBy(product_col).count().withColumnRenamed('count', 'count_1')
        
        # Adjust counts based on specified ratio and at_least parameter
        df_group_flag_1 = df_group_flag_1.withColumn('adjusted_count', col('count_1') * zero_ratio)

        # Aliasing the DataFrames and using a DataFrame join condition
        df1 = df_group_flag_1.alias('df1')
        df0 = df_group_flag_0.alias('df0')
        df_group_flag = df1.join(df0, df1[product_col] == df0[product_col], 'outer')

        # Specify which DataFrame's column to use when filling NA values
        # This assumes the joined DataFrame retains the alias
        df_group_flag = df_group_flag.na.fill({
            'df0.count_0': at_least / zero_ratio, 
            'adjusted_count': at_least,
            'df1.count_1': 0
        })

        # Specify which DataFrame's product_col to use for further operations
        fraction_col = col('adjusted_count') / col('df0.count_0')
        df_group_flag = df_group_flag.withColumn('fraction_col', fraction_col)
        fraction_dict = df_group_flag.select(df1[product_col].alias(product_col), 'fraction_col').rdd.collectAsMap()
        
        # Apply subsampling to Flag=0 data
        sample_data_0 = df_flag_0.sampleBy(product_col, fractions=fraction_dict, seed=int(seed))
        sample_data = sample_data_0.union(df_flag_1)
        
        subsampled_data_dict[seed] = sample_data
    
    return subsampled_data_dict




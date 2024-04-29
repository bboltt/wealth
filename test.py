import pandas as pd

def mark_top_10_percent(df, ip_id_col, prd_name_col, score_col):
    # Step 1: Remove duplicates based on 'ip_id' and 'prd_name' for accurate top 10% calculation, keeping the highest score
    unique_products = df.groupby([ip_id_col, prd_name_col])[score_col].max().reset_index()

    # Step 2: Sort by score and take the top 10% of unique products per 'ip_id'
    top_products = unique_products.groupby(ip_id_col).apply(
        lambda x: x.nlargest(int(len(x) * 0.1 + 0.9), score_col)
    ).reset_index(drop=True)  # using 0.9 to ensure at least one product is chosen if rounding is an issue

    # Step 3: Mark these as top products
    top_products['recommend'] = 1

    # Step 4: Merge this information back with the original DataFrame
    result_df = pd.merge(df, top_products[[ip_id_col, prd_name_col, 'recommend']], on=[ip_id_col, prd_name_col], how='left')

    # Step 5: Fill in the non-top products and ensure the column is integer
    result_df['recommend'] = result_df['recommend'].fillna(0).astype(int)

    return result_df

# Example usage
data = {
    'ip_id': [1, 1, 1, 2, 2, 3, 3, 3, 3],
    'prd_name': ['A', 'A', 'B', 'A', 'B', 'C', 'C', 'D', 'E'],
    'score': [90, 90, 80, 85, 95, 70, 70, 60, 55]
}
df = pd.DataFrame(data)

# Call the function
result_df = mark_top_10_percent(df, 'ip_id', 'prd_name', 'score')
print(result_df)

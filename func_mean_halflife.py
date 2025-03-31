import pandas as pd
import math
import statsmodels.api as sm

def calculate_half_life(series):
    """
    Estimate the half-life of mean reversion for a time series.
    Uses the regression: Δx_t = β * x_(t-1) + intercept,
    then half_life = -ln(2) / beta (if beta is negative).
    """
    # Drop missing values
    series = series.dropna()
    if series.empty:
        return None
    # Create lagged series and compute delta
    lag = series.shift(1)
    delta = series - lag
    # Combine and drop missing rows
    df_temp = pd.concat([delta, lag], axis=1).dropna()
    delta = df_temp.iloc[:, 0]
    lag = df_temp.iloc[:, 1]
    # Add constant for intercept
    lag_const = sm.add_constant(lag)
    model = sm.OLS(delta, lag_const).fit()
    beta = model.params[1]
    if beta >= 0:
        # Process is not mean-reverting; return NaN.
        return float('nan')
    half_life = -math.log(2) / beta
    return half_life

def compute_mean_and_halflife(csv_file='df_top50_coint.csv'):
    """
    Loads 'df_top50_coint.csv' (with 'datetime' as the index),
    then computes for each column ending with '_zscore' the simple mean and half-life.
    Returns a one-row DataFrame with columns like:
      '<pair>_mean_zscore', '<pair>_halflife'
    The computed values are rounded to 2 decimal places.
    """
    df = pd.read_csv(csv_file, parse_dates=True, index_col='datetime')
    # Identify all zscore columns.
    zscore_cols = [col for col in df.columns if col.endswith('_zscore')]
    
    results = {}
    for col in zscore_cols:
        mean_z = df[col].mean()
        half_life = calculate_half_life(df[col])
        pair_name = col.replace('_zscore', '')
        results[f"{pair_name}_mean_zscore"] = round(mean_z, 2) if pd.notnull(mean_z) else None
        results[f"{pair_name}_halflife"] = round(half_life, 2) if pd.notnull(half_life) else None
        
    result_df = pd.DataFrame(results, index=[0])
    return result_df

if __name__ == '__main__':
    # Compute mean and half-life metrics
    result_df = compute_mean_and_halflife('df_top50_coint.csv')
    
    # -----------------------------------------------------------
    # Sort the columns by half-life (descending)
    # -----------------------------------------------------------
    # 1. Identify half-life columns.
    half_cols = [col for col in result_df.columns if col.endswith('_halflife')]
    # 2. Extract the values from the single row.
    half_series = result_df.loc[0, half_cols]
    # 3. Sort the half-life values descending.
    sorted_half = half_series.sort_values(ascending=False)
    # 4. Get the corresponding pair names.
    sorted_pairs = [col.replace('_halflife', '') for col in sorted_half.index]
    
    # 5. Reorder the result_df columns accordingly.
    ordered_cols = []
    for pair in sorted_pairs:
        mean_col = f"{pair}_mean_zscore"
        halflife_col = f"{pair}_halflife"
        if mean_col in result_df.columns and halflife_col in result_df.columns:
            ordered_cols.append(mean_col)
            ordered_cols.append(halflife_col)
    
    result_df_sorted = result_df[ordered_cols]
        
    # Save the sorted results to CSV.
    result_df_sorted.to_csv("df_mean_halflife.csv", index=False)
    print("Saved sorted results to 'df_mean_halflife.csv'.")
    # print(result_df_sorted)

    # Extract only the columns that end with '_halflife'
    half_cols = [col for col in result_df.columns if col.endswith('_halflife')]
    half_series = result_df.loc[0, half_cols]
    
    # Sort the half-life values in descending order.
    sorted_half_series = half_series.sort_values(ascending=False)
    
    # Print the sorted half-life values.
    print("Sorted Half-life values (descending):")
    print(sorted_half_series)
    
    # Save the sorted half-life values to CSV.
    sorted_half_series.to_csv("df_halflife_only.csv", header=True)
    print("Saved sorted half-life values to 'df_halflife_only.csv'.")

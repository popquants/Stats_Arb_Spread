import pandas as pd
from func_cointegration import extract_close_prices, calculate_spread, calculate_zscore

# Load cointegrated pairs data and select top 50 (sorted by zero_crossings descending)
df_pairs = pd.read_csv('df_cointegrated_pairs.csv')
top50 = df_pairs.sort_values('zero_crossings', ascending=False).head(50)

# Load wide-format price data (each column is a trading pair's close price)
wide_df = pd.read_csv('wide_df.csv', index_col='datetime', parse_dates=True)

# Reset index so that the index is numeric (this ensures that the computed series and final_df align)
wide_df_reset = wide_df.reset_index()

# Create final DataFrame with the same index as wide_df_reset.
final_df = pd.DataFrame(index=wide_df_reset.index)
final_df['datetime'] = wide_df_reset['datetime']

# Loop through each pair in the top 50 list.
for idx, row in top50.iterrows():
    sym1 = row['sym_1']
    sym2 = row['sym_2']
    hedge_ratio = row['hedge_ratio']
    pair_name = f"{sym1}:{sym2}"
    
    # Check that both symbols exist in wide_df_reset.
    if sym1 not in wide_df_reset.columns or sym2 not in wide_df_reset.columns:
        continue

    # Extract the price data for the pair and drop missing values.
    df_pair = wide_df_reset[[sym1, sym2]].dropna()
    if df_pair.empty:
        continue
    
    # Extract prices (as numpy arrays) using your extraction function.
    prices1 = extract_close_prices(df_pair[sym1])
    prices2 = extract_close_prices(df_pair[sym2])
    
    # Calculate spread and zscore using the hedge ratio.
    spread = calculate_spread(prices1, prices2, hedge_ratio)
    zscore = calculate_zscore(spread)
    
    # Build Series for the computed values using the index of df_pair.
    spread_series = pd.Series(spread, index=df_pair.index)
    zscore_series = pd.Series(zscore, index=df_pair.index)
    
    # Directly assign these values into final_df for the indices present in df_pair.
    final_df.loc[df_pair.index, f"{pair_name}_spread"] = spread_series
    final_df.loc[df_pair.index, f"{pair_name}_zscore"] = zscore_series

# Sort the final DataFrame by the numeric index and save.
final_df.sort_index(inplace=True)
final_df.to_csv("df_top50_coint.csv", index=False)
print("Saved final results to 'df_top50_coint.csv'.")

# ---------------------------------------------------------
# Print the top 10 highest and lowest z-score trading pairs
# based on the LAST row in final_df.
# ---------------------------------------------------------

# 1) Identify all columns that end with "_zscore"
zscore_cols = [col for col in final_df.columns if col.endswith("_zscore") and col != "datetime"]

# 2) Get the last row for these columns.
if not final_df.empty:
    last_row_index = final_df.index[-1]
    zscore_values = final_df.loc[last_row_index, zscore_cols]
    
    # 3) Build a Series mapping "sym1:sym2" -> zscore_value
    pairs_zscore = {}
    for col in zscore_cols:
        # Example: "BTC/USDT:ETH/USDT_zscore" becomes "BTC/USDT:ETH/USDT"
        pair_name = col.replace("_zscore", "")
        val = zscore_values[col]
        pairs_zscore[pair_name] = val

    zscore_series = pd.Series(pairs_zscore)

    # 4) Sort descending to find highest and ascending for lowest.
    zscore_sorted = zscore_series.sort_values(ascending=False)

    # 5) Print top 10 highest
    print("\nTop 10 Highest Z-Score (LAST ROW):")
    for pair, val in zscore_sorted.head(10).items():
        print(f"{pair} => {val:.4f}")

    # 6) Print top 10 lowest
    print("\nTop 10 Lowest Z-Score (LAST ROW):")
    for pair, val in zscore_sorted.tail(10).sort_values().items():
        print(f"{pair} => {val:.4f}")
else:
    print("final_df is empty. No z-score data to display.")

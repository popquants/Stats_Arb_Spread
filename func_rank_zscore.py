"""
Calculate z-scores for cointegrated pairs.
"""

import pandas as pd
import numpy as np
from func_cointegration import extract_close_prices, calculate_spread, calculate_zscore

def process_pairs():
    """
    Process cointegrated pairs and calculate their z-scores.
    This function should be called after df_cointegrated_pairs.csv is created.
    """
    try:
        # Load cointegrated pairs data and select top 50 (sorted by zero_crossings descending)
        df_pairs = pd.read_csv('df_cointegrated_pairs.csv')
        top50 = df_pairs.sort_values('zero_crossings', ascending=False).head(50)

        # Load wide-format price data (each column is a trading pair's close price)
        wide_df = pd.read_csv('wide_df.csv', index_col='datetime', parse_dates=True)
        
        # Debug print
        print(f"Loaded wide_df with shape: {wide_df.shape}")
        print(f"Columns in wide_df: {wide_df.columns.tolist()[:5]}...")

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
                print(f"Skipping pair {pair_name} - symbols not found in data")
                continue

            # Extract the price data for the pair and drop missing values.
            df_pair = wide_df_reset[[sym1, sym2]].dropna()
            if df_pair.empty:
                print(f"Skipping pair {pair_name} - no valid data after dropping NaN")
                continue
            
            # Debug print
            print(f"\nProcessing pair {pair_name}:")
            print(f"Data shape: {df_pair.shape}")
            print(f"Price ranges: {sym1}: [{df_pair[sym1].min():.2f}, {df_pair[sym1].max():.2f}]")
            print(f"Price ranges: {sym2}: [{df_pair[sym2].min():.2f}, {df_pair[sym2].max():.2f}]")
            
            # Calculate spread directly from the DataFrame
            spread = df_pair[sym1] - df_pair[sym2] * hedge_ratio
            
            # Calculate z-score using rolling window from func_cointegration
            zscore = calculate_zscore(spread.values)
            
            # Debug print
            print(f"Spread range: [{spread.min():.2f}, {spread.max():.2f}]")
            print(f"Z-score range: [{np.min(zscore):.2f}, {np.max(zscore):.2f}]")
            
            # Build Series for the computed values using the index of df_pair.
            spread_series = pd.Series(spread, index=df_pair.index)
            zscore_series = pd.Series(zscore, index=df_pair.index)
            
            # Directly assign these values into final_df for the indices present in df_pair.
            final_df.loc[df_pair.index, f"{pair_name}_spread"] = spread_series
            final_df.loc[df_pair.index, f"{pair_name}_zscore"] = zscore_series

        # Sort the final DataFrame by the numeric index and save.
        final_df.sort_index(inplace=True)
        
        # Ensure datetime column is properly formatted
        final_df['datetime'] = pd.to_datetime(final_df['datetime'])
        
        # Save with datetime column
        final_df.to_csv("df_top50_coint.csv", index=False)
        print("\nSaved final results to 'df_top50_coint.csv'")

        # Calculate mean z-scores for each pair
        mean_zscore_df = pd.DataFrame()
        mean_zscore_df['datetime'] = final_df['datetime']
        
        # Get all zscore columns
        zscore_cols = [col for col in final_df.columns if col.endswith("_zscore")]
        
        # Calculate mean z-score for each pair
        for col in zscore_cols:
            pair_name = col.replace("_zscore", "")
            mean_zscore_df[f"{pair_name}_mean_zscore"] = final_df[col].rolling(window=20).mean()
        
        # Save mean z-scores
        mean_zscore_df.to_csv("df_mean_halflife.csv", index=False)
        print("Saved mean z-scores to 'df_mean_halflife.csv'")

        # Print the top 10 highest and lowest z-score trading pairs
        if not final_df.empty:
            last_row_index = final_df.index[-1]
            zscore_values = final_df.loc[last_row_index, zscore_cols]
            
            # Build a Series mapping "sym1:sym2" -> zscore_value
            pairs_zscore = {}
            for col in zscore_cols:
                pair_name = col.replace("_zscore", "")
                val = zscore_values[col]
                pairs_zscore[pair_name] = val

            zscore_series = pd.Series(pairs_zscore)
            zscore_sorted = zscore_series.sort_values(ascending=False)

            print("\nTop 10 Highest Z-Score (LAST ROW):")
            for pair, val in zscore_sorted.head(10).items():
                print(f"{pair} => {val:.4f}")

            print("\nTop 10 Lowest Z-Score (LAST ROW):")
            for pair, val in zscore_sorted.tail(10).sort_values().items():
                print(f"{pair} => {val:.4f}")
        else:
            print("final_df is empty. No z-score data to display.")
            
    except FileNotFoundError as e:
        print(f"Required file not found: {str(e)}")
        print("Please ensure all required files exist before running this function.")
    except Exception as e:
        print(f"Error processing pairs: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

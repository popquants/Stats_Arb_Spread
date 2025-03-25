import warnings
warnings.filterwarnings("ignore")

from statsmodels.tsa.stattools import coint
import statsmodels.api as sm
import pandas as pd
import numpy as np
import math
from tqdm import tqdm 

# Define a default z-score window for daily data
z_score_window = 20  # 20 days for daily timeframe

def extract_close_prices(prices):
    """
    Extracts close prices from the provided data.
    """
    try:
        if isinstance(prices, list):
            close_prices = []
            for price_item in prices:
                if isinstance(price_item, dict) and "close" in price_item:
                    if math.isnan(price_item["close"]):
                        return []
                    close_prices.append(price_item["close"])
                else:
                    close_prices.append(price_item)
            return close_prices
        elif isinstance(prices, pd.Series):
            return prices.values
        elif isinstance(prices, np.ndarray):
            return prices
        else:
            return list(prices)
    except Exception as e:
        print(f"Error extracting close prices: {e}")
        return []

# Calculate Z-Score from a spread series.
def calculate_zscore(spread):
    df = pd.DataFrame(spread, columns=['spread'])
    mean = df.rolling(window=z_score_window).mean()
    std = df.rolling(window=z_score_window).std()
    # Rolling window of 1 is just the value itself.
    x = df.rolling(window=1).mean()
    df["ZSCORE"] = (x - mean) / std
    return df["ZSCORE"].astype(float).values

# Calculate spread given two series and a hedge ratio.
def calculate_spread(series_1, series_2, hedge_ratio):
    spread = pd.Series(series_1) - pd.Series(series_2) * hedge_ratio
    return spread

# Calculate cointegration between two series.
def calculate_cointegration(series_1, series_2):
    coint_flag = 0
    # Ensure both series are of equal length.
    min_len = min(len(series_1), len(series_2))
    series_1 = series_1[:min_len]
    series_2 = series_2[:min_len]
    
    # Run cointegration test.
    coint_res = coint(series_1, series_2)
    coint_t = coint_res[0]
    p_value = coint_res[1]
    critical_value = coint_res[2][1]  # 95% critical value.
    
    # Calculate hedge ratio using OLS (without intercept).
    model = sm.OLS(series_1, series_2).fit()
    hedge_ratio = model.params[0]
    
    # Compute the spread and count its zero crossings.
    spread = calculate_spread(series_1, series_2, hedge_ratio)
    zero_crossings = len(np.where(np.diff(np.sign(spread)))[0])
    
    # A simple cointegration criterion.
    if p_value < 0.5 and coint_t < critical_value:
        coint_flag = 1
    return (coint_flag, round(p_value, 2), round(coint_t, 2),
            round(critical_value, 2), round(hedge_ratio, 2), zero_crossings)

# Calculate cointegrated pairs from the close prices DataFrame.
def get_cointegrated_pairs(close_df):
    coint_pair_list = []
    included_list = []
    symbols = close_df.columns
    total_pairs = (len(symbols) * (len(symbols) - 1)) // 2

    # Initialize progress bar.
    progress_bar = tqdm(total=total_pairs, desc="Processing pairs", unit="pair")
    
    for i in range(len(symbols)):
        for j in range(i + 1, len(symbols)):
            sym1 = symbols[i]
            sym2 = symbols[j]
            try:
                # Align the series by keeping only rows where both symbols have data.
                pair_df = close_df[[sym1, sym2]].dropna()
                if pair_df.empty:
                    continue
                series_1 = pair_df[sym1].values
                series_2 = pair_df[sym2].values

                # Calculate cointegration and related statistics.
                (coint_flag, p_value, t_value, c_value, 
                 hedge_ratio, zero_crossings) = calculate_cointegration(series_1, series_2)
                
                if coint_flag == 1:
                    # Create a unique identifier for the pair.
                    unique = "".join(sorted([sym1, sym2]))
                    if unique not in included_list:
                        included_list.append(unique)
                        coint_pair_list.append({
                            "sym_1": sym1,
                            "sym_2": sym2,
                            "p_value": p_value,
                            "t_value": t_value,
                            "c_value": c_value,
                            "hedge_ratio": hedge_ratio,
                            "zero_crossings": zero_crossings
                        })
            except Exception as e:
                print(f"Error processing pair {sym1} and {sym2}: {e}")
            finally:
                progress_bar.update(1)
    progress_bar.close()

    df_coint = pd.DataFrame(coint_pair_list)
    if not df_coint.empty:
        df_coint = df_coint.sort_values("zero_crossings", ascending=False)
    df_coint.to_csv("df_cointegrated_pairs_daily.csv", index=False)
    return df_coint

if __name__ == '__main__':
    try:
        # Read daily data from CSV.
        close_df = pd.read_csv('wide_df_1d.csv', index_col='datetime', parse_dates=True)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        exit(1)
    
    try:
        # Calculate and save the cointegrated pairs.
        cointegrated_pairs_df = get_cointegrated_pairs(close_df)
        print("Cointegrated pairs saved to 'df_cointegrated_pairs_daily.csv'.")
    except Exception as e:
        print(f"Error calculating cointegrated pairs: {e}")

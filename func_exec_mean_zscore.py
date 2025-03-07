import traceback
import time
import ccxt
import configparser
import pandas as pd
import numpy as np
import math

# ---------------------------
# Load API keys from config.ini
# ---------------------------
config = configparser.ConfigParser()
config.read('config.ini')

api_key = config.get('BINANCE', 'API_KEY', fallback=None)
api_secret = config.get('BINANCE', 'API_SECRET', fallback=None)

if not api_key or not api_secret:
    raise ValueError("API Key and Secret must be set in the config.ini file under the [BINANCE] section.")

# Initialize Binance Futures (USDT) with auto time synchronization.
binance_futures = ccxt.binanceusdm({
    'apiKey': api_key,
    'secret': api_secret,
    'enableRateLimit': True,
    'options': {
        'adjustForTimeDifference': True
    }
})

def place_orders(long_pair, short_pair, long_amount, short_amount):
    """
    Place a long and short order on the futures market.
    """
    try:
        print("\n--- Fetching Balances ---")
        futures_balance_info = binance_futures.fetch_balance(params={'type': 'future'})
        futures_free_margin = float(futures_balance_info['free']['USDT'])
        print(f"Futures Free Margin: {futures_free_margin:.2f} USDT")

        total_required = long_amount + short_amount
        if total_required > futures_free_margin:
            raise ValueError(f"Insufficient futures free margin. Available: {futures_free_margin:.2f}, Required: {total_required:.2f}")

        print("\n--- Fetching Current Prices ---")
        long_ticker = binance_futures.fetch_ticker(long_pair)
        short_ticker = binance_futures.fetch_ticker(short_pair)
        long_price = long_ticker['last']
        short_price = short_ticker['last']
        print(f"Long Pair ({long_pair}) Price: {long_price:.4f} USDT")
        print(f"Short Pair ({short_pair}) Price: {short_price:.4f} USDT")

        long_quantity = long_amount / long_price
        short_quantity = short_amount / short_price
        print(f"Placing Orders: Long {long_pair} ({long_quantity:.4f}) and Short {short_pair} ({short_quantity:.4f})")

        print("\n--- Placing Long Order ---")
        long_order = binance_futures.create_order(
            symbol=long_pair,
            type='market',
            side='buy',
            amount=long_quantity
        )
        print(f"Long Order Placed: {long_order['id']} | Status: {long_order['status']} | Filled: {long_order['filled']}")

        print("\n--- Placing Short Order ---")
        short_order = binance_futures.create_order(
            symbol=short_pair,
            type='market',
            side='sell',
            amount=short_quantity
        )
        print(f"Short Order Placed: {short_order['id']} | Status: {short_order['status']} | Filled: {short_order['filled']}")

        return [long_order, short_order]

    except ccxt.InsufficientFunds as e:
        print(f"\nError: Insufficient funds - {e}")
        print("Tip: Check your futures free margin or reduce the trade amount.")
    except Exception as e:
        print(f"\nError: {e}")
        traceback.print_exc()
        return []

def close_orders(orders):
    """
    Closes the given orders by canceling them using the Binance Futures API.
    """
    try:
        print("\n--- Closing Orders ---")
        for order in orders:
            symbol = order['symbol']
            order_id = order['id']
            print(f"Cancelling order {order_id} for {symbol}...")
            cancel_result = binance_futures.cancel_order(order_id, symbol)
            print(f"Order {order_id} cancelled. Result: {cancel_result}")
    except Exception as e:
        print(f"Error closing orders: {e}")
        traceback.print_exc()

def get_current_zscore(pair1, pair2, hedge_ratio, window=20, timeframe='1h'):
    """
    Fetches the most recent OHLCV data for two symbols from Binance,
    computes the spread as: spread = close(pair1) - hedge_ratio * close(pair2),
    calculates the rolling mean and standard deviation over the last 'window' candles,
    and returns the z-score of the most recent spread.
    
    Parameters:
      - pair1 (str): e.g., "ALT/USDT"
      - pair2 (str): e.g., "ZIL/USDT"
      - hedge_ratio (float): Retrieved from df_cointegrated_pairs.csv.
      - window (int): Number of recent candles to use (default: 20)
      - timeframe (str): OHLCV timeframe to fetch (default: '1h')
      
    Returns:
      float: The current z-score, or None if an error occurs.
    """
    try:
        binance_futures.load_markets()
        ohlcv1 = binance_futures.fetch_ohlcv(pair1, timeframe=timeframe, limit=window)
        ohlcv2 = binance_futures.fetch_ohlcv(pair2, timeframe=timeframe, limit=window)
        
        closes1 = [candle[4] for candle in ohlcv1]
        closes2 = [candle[4] for candle in ohlcv2]
        
        if len(closes1) < window or len(closes2) < window:
            raise ValueError("Not enough data to compute z-score.")
        
        spread = [c1 - hedge_ratio * c2 for c1, c2 in zip(closes1, closes2)]
        
        mean_spread = sum(spread) / len(spread)
        std_spread = math.sqrt(sum((x - mean_spread) ** 2 for x in spread) / len(spread))
        
        if std_spread == 0:
            return 0.0
        
        current_zscore = (spread[-1] - mean_spread) / std_spread
        return current_zscore
    
    except Exception as e:
        print(f"Error computing z-score for {pair1}:{pair2}: {e}")
        traceback.print_exc()
        return None

def get_mean_zscore(long_pair, short_pair):
    """
    Loads 'df_mean_halflife.csv' and returns the mean zscore for the given pair.
    The CSV is assumed to have one row with columns like:
    "ALT/USDT:ZIL/USDT_mean_zscore"
    The pair key is built by sorting the two inputs and joining them with a colon,
    then appending "_mean_zscore".
    """
    try:
        df_zmean = pd.read_csv("df_mean_halflife.csv")
        pairs = sorted([long_pair.strip(), short_pair.strip()])
        key = f"{pairs[0]}:{pairs[1]}_mean_zscore"
        if key not in df_zmean.columns:
            print(f"Mean zscore for {key} not found in df_mean_halflife.csv.")
            return None
        return float(df_zmean.loc[0, key])
    except Exception as e:
        print(f"Error loading mean zscore: {e}")
        traceback.print_exc()
        return None

def main():
    print("Welcome to the Crypto Arbitrage Program!")

    # Prompt user for input trading pairs and amounts.
    long_pair = input("Enter the Long pair (will open a Long position, e.g., ALT/USDT): ").strip()
    short_pair = input("Enter the Short pair (will open a Short position, e.g., ZIL/USDT): ").strip()
    long_amount = float(input("Enter the dollar amount for the Long position (e.g., 100): "))
    short_amount = float(input("Enter the dollar amount for the Short position (e.g., 90): "))

    print("\nSummary of your input:")
    print(f"Long Pair: {long_pair}")
    print(f"Short Pair: {short_pair}")
    print(f"Long Amount: {long_amount:.2f} USDT")
    print(f"Short Amount: {short_amount:.2f} USDT")

    # Load hedge ratio from 'df_cointegrated_pairs.csv'
    df_hr = pd.read_csv("df_cointegrated_pairs.csv")
    # Standardize the pair order by sorting.
    pairs = sorted([long_pair.strip(), short_pair.strip()])
    # Look for either order in the CSV.
    row = df_hr[((df_hr["sym_1"] == pairs[0]) & (df_hr["sym_2"] == pairs[1])) |
                ((df_hr["sym_1"] == pairs[1]) & (df_hr["sym_2"] == pairs[0]))]
    if row.empty:
        print("Could not find hedge ratio for the given pair combination.")
        return
    hedge_ratio = row["hedge_ratio"].values[0]
    print(f"Using hedge ratio: {hedge_ratio}")

    # Load mean zscore from 'df_mean_halflife.csv'
    mean_zscore_value = get_mean_zscore(long_pair, short_pair)
    if mean_zscore_value is None:
        print("Could not retrieve mean zscore for the given pair. Exiting.")
        return
    print(f"Retrieved mean z-score for the pair: {mean_zscore_value:.2f}")

    print("Executing trades...")
    placed_orders = place_orders(long_pair, short_pair, long_amount, short_amount)

    # Monitor current z-score and auto-close orders when condition is met.
    if placed_orders:
        print("\nMonitoring current z-score for auto-close conditions...")
        while True:
            current_z = get_current_zscore(long_pair, short_pair, hedge_ratio, window=20, timeframe='1h')
            if current_z is None:
                print("Failed to compute current z-score, trying again in 5 minutes.")
            else:
                print(f"Current z-score for {long_pair}:{short_pair} = {current_z:.2f}")
                # If current zscore > 0, then close orders when current_z <= mean_zscore.
                # If current zscore < 0, then close orders when current_z >= mean_zscore.
                if (current_z > 0 and current_z <= mean_zscore_value) or \
                   (current_z < 0 and current_z >= mean_zscore_value):
                    print("Z-score condition met. Closing orders...")
                    close_orders(placed_orders)
                    break
            time.sleep(300)  # Check every 5 minutes.

if __name__ == "__main__":
    main()

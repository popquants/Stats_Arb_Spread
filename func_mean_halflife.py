import traceback
import time
import ccxt
import configparser
import pandas as pd
import numpy as np
import math

# ---------------------------
# Load API keys from a config file
# ---------------------------
config = configparser.ConfigParser()
config.read('config.ini')

api_key = config.get('BINANCE', 'API_KEY', fallback=None)
api_secret = config.get('BINANCE', 'API_SECRET', fallback=None)

if not api_key or not api_secret:
    raise ValueError("API Key and Secret must be set in the config.ini file under the [BINANCE] section.")

# Initialize the Binance Futures API with auto time synchronization
binance_futures = ccxt.binanceusdm({
    'apiKey': api_key,
    'secret': api_secret,
    'enableRateLimit': True,
    'options': {
        'adjustForTimeDifference': True  # Automatically sync time difference
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

def close_positions(orders):
    """
    Closes the active positions by placing reverse orders with the 'reduceOnly' flag.
    This assumes that each order in 'orders' was fully executed.
    """
    try:
        print("\n--- Closing Positions ---")
        for order in orders:
            symbol = order['symbol']
            original_side = order['side']
            # Determine the opposite side: sell to close a long, buy to close a short.
            close_side = 'sell' if original_side == 'buy' else 'buy'
            # Use the 'filled' amount if available; otherwise, fall back to the original 'amount'
            amount = order.get('filled', order['amount'])
            print(f"Closing position for {symbol}: placing {close_side} order for {amount:.4f}")
            close_order = binance_futures.create_order(
                symbol=symbol,
                type='market',
                side=close_side,
                amount=amount,
                params={'reduceOnly': True}
            )
            print(f"Close order placed for {symbol}: {close_order['id']} | Status: {close_order['status']}")
    except Exception as e:
        print(f"Error closing positions: {e}")
        traceback.print_exc()

def main():
    print("Welcome to the Crypto Arbitrage Program!")

    # Prompt the user for inputs
    long_pair = input("Enter the crypto pair to Long (e.g., BTC/USDT): ").strip()
    short_pair = input("Enter the crypto pair to Short (e.g., ETH/USDT): ").strip()
    long_amount = float(input("Enter the dollar amount for the Long position (e.g., 100): "))
    short_amount = float(input("Enter the dollar amount for the Short position (e.g., 90): "))

    print("\nSummary of your input:")
    print(f"Long Pair: {long_pair}")
    print(f"Short Pair: {short_pair}")
    print(f"Long Amount: {long_amount:.2f} USDT")
    print(f"Short Amount: {short_amount:.2f} USDT")

    # Load half-life value from df_halflife_only.csv.
    # Assume the CSV has two columns: 'pair' and 'value'
    df_halflife = pd.read_csv("df_halflife_only.csv", header=None, names=["pair", "value"])
    # Standardize the pair key: sort the two inputs and join with a colon, then append '_halflife'
    pairs = sorted([long_pair, short_pair])
    pair_key = f"{pairs[0]}:{pairs[1]}_halflife"
    row = df_halflife[df_halflife["pair"] == pair_key]
    if row.empty:
        print(f"Could not find half-life value for pair {pair_key} in df_halflife_only.csv.")
        return
    half_life_value = float(row["value"].values[0])
    half_life_rounded = math.ceil(half_life_value)
    
    print(f"\nRetrieved half-life for {pair_key}: {half_life_value:.2f} hours (rounded up to {half_life_rounded} hours).")
    user_choice = input(f"Do you want to close positions automatically after {half_life_rounded} hours? (yes/no): ").strip().lower()
    if user_choice != "yes":
        print("Auto-close cancelled. Exiting.")
        return

    print("Executing trades...")
    placed_orders = place_orders(long_pair, short_pair, long_amount, short_amount)

    # Schedule auto-close using the half-life value retrieved from the CSV.
    if placed_orders:
        print(f"Waiting {half_life_rounded} hours to automatically close positions...")
        time.sleep(half_life_rounded * 3600)
        print("Half-life period reached. Closing positions...")
        close_positions(placed_orders)

if __name__ == "__main__":
    main()

import traceback
import time
import ccxt

api_key = api_key 
api_secret = api_secret

# Initialize the Binance API with auto time synchronization
binance_futures = ccxt.binanceusdm({
    'apiKey': api_key,
    'secret': api_secret,
    'enableRateLimit': True,
    'options': {
        'adjustForTimeDifference': True  # Automatically sync time difference
    }
})

def place_orders(long_pair, short_pair, percentage):
    """
    Place a long and short order on the futures market.
    """
    try:
        print("\n--- Fetching Balances ---")
        futures_balance_info = binance_futures.fetch_balance(params={'type': 'future'})
        futures_free_margin = float(futures_balance_info['free']['USDT'])
        print(f"Futures Free Margin: {futures_free_margin:.2f} USDT")

        # Calculate trade amounts
        print("\n--- Calculating Trade Amounts ---")
        futures_trade_amount = (percentage / 100) * futures_free_margin
        print(f"Calculated Futures Trade Amount: {futures_trade_amount:.2f} USDT")

        if futures_trade_amount > futures_free_margin:
            raise ValueError(f"Insufficient futures free margin. Available: {futures_free_margin:.2f}, Required: {futures_trade_amount:.2f}")

        # Fetch the current prices for long and short pairs
        print("\n--- Fetching Current Prices ---")
        long_price = binance_futures.fetch_ticker(long_pair)['last']
        short_price = binance_futures.fetch_ticker(short_pair)['last']
        print(f"Long Pair ({long_pair}) Price: {long_price:.4f} USDT")
        print(f"Short Pair ({short_pair}) Price: {short_price:.4f} USDT")

        # Calculate quantities to trade
        long_quantity = futures_trade_amount / long_price
        short_quantity = futures_trade_amount / short_price
        print(f"Placing Orders: Long {long_pair} ({long_quantity:.4f}) and Short {short_pair} ({short_quantity:.4f})")

        # Place long order
        print("\n--- Placing Long Order ---")
        long_order = binance_futures.create_order(
            symbol=long_pair,
            type='market',
            side='buy',
            amount=long_quantity
        )
        print(f"Long Order Placed: {long_order['id']} | Status: {long_order['status']} | Filled: {long_order['filled']}")

        # Place short order
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
        print("Tip: Check your futures free margin or reduce the trade percentage.")
    except Exception as e:
        print(f"\nError: {e}")
        traceback.print_exc()
        return []

def close_orders(orders):
    """
    Close all the provided orders.
    """
    try:
        for order in orders:
            symbol = order['symbol']
            side = 'sell' if order['side'] == 'buy' else 'buy'
            amount = order['amount']

            binance_futures.create_order(
                symbol=symbol,
                type='market',
                side=side,
                amount=amount
            )
            print(f"Closed order for {symbol}, Amount: {amount:.4f}")
    except Exception as e:
        print(f"Error closing orders: {e}")

def main():
    print("Welcome to the Crypto Trade Program!")

    # Prompt the user for inputs
    long_pair = input("Enter the crypto pair to Long (e.g., XXX/USDT): ")
    short_pair = input("Enter the crypto pair to Short (e.g., YYY/USDT): ")
    percentage = float(input("Enter the percentage of your wallet to use (e.g., 50): "))
    half_life = float(input("Enter the half-life in hours to automatically close orders (e.g., 2.5): "))

    print("\nSummary of your input:")
    print(f"Long Pair: {long_pair}")
    print(f"Short Pair: {short_pair}")
    print(f"Percentage: {percentage}%")
    print(f"Half-Life: {half_life} hours\n")

    confirm = input("Do you want to proceed with these values? (yes/no): ").strip().lower()

    if confirm == "yes":
        print("Executing trades...")
        placed_orders = place_orders(long_pair, short_pair, percentage)

        if placed_orders:
            print(f"Waiting {half_life} hours to automatically close orders...")
            time.sleep(half_life * 3600)
            print("Closing orders...")
            close_orders(placed_orders)
    else:
        print("Trade execution cancelled.")

if __name__ == "__main__":
    main()

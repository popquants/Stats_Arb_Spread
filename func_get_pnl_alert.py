import traceback
import time
import ccxt
import configparser
from songline import Sendline

# Load API keys from a config file
config = configparser.ConfigParser()
config.read('config.ini')

api_key = config.get('BINANCE', 'API_KEY', fallback=None)
api_secret = config.get('BINANCE', 'API_SECRET', fallback=None)
line_token = config.get('LINE', 'TOKEN', fallback=None)

if not api_key or not api_secret:
    raise ValueError("API Key and Secret must be set in the config.ini file under the [BINANCE] section.")
if not line_token:
    raise ValueError("LINE Notify token must be set in the config.ini file under the [LINE] section.")

# Initialize the Binance API with auto time synchronization
binance_futures = ccxt.binanceusdm({
    'apiKey': api_key,
    'secret': api_secret,
    'enableRateLimit': True,
    'options': {
        'adjustForTimeDifference': True  # Automatically sync time difference
    }
})

# Initialize Songline
line = Sendline(line_token)

def send_line_alert(message):
    """
    Send an alert to LINE using the Songline library.
    """
    try:
        line.sendtext(message)
        print("LINE alert sent successfully!")
    except Exception as e:
        print(f"Error sending LINE alert: {e}")
        traceback.print_exc()

def get_unrealized_pnl():
    """
    Fetch the unrealized PNL for all open futures positions and summarize the total.
    """
    try:
        print("\n--- Fetching Futures Positions ---")
        positions = binance_futures.fetch_positions()

        total_pnl = 0.0
        pnl_data = []

        for position in positions:
            symbol = position['symbol']
            unrealized_pnl = float(position['unrealizedPnl'])
            pnl_data.append((symbol, unrealized_pnl))
            total_pnl += unrealized_pnl

        print("\nUnrealized PNL for Open Positions:")
        for symbol, pnl in pnl_data:
            print(f"{symbol}: {pnl:.2f} USDT")

        print(f"\nTotal Unrealized PNL: {total_pnl:.2f} USDT")

        if total_pnl > 0:
            print("Your positions are in profit.")
            if total_pnl > 1:
                send_line_alert(f"🎉 Profit Alert: Your total unrealized PNL is {total_pnl:.2f} USDT.")
        elif total_pnl < 0:
            print("Your positions are at a loss.")
        else:
            print("Your positions are breakeven.")

        return pnl_data, total_pnl

    except Exception as e:
        print(f"Error fetching unrealized PNL: {e}")
        traceback.print_exc()
        return [], 0.0

if __name__ == "__main__":
    print("Welcome to the Binance Futures Unrealized PNL Viewer!")
    while True:
        get_unrealized_pnl()
        print("\nWaiting for 1 minutes before the next check...")
        time.sleep(180)

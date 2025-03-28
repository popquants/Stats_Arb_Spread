import traceback
import time
import ccxt
import configparser
import pandas as pd
import numpy as np

# Load API keys from a config file
config = configparser.ConfigParser()
config.read('config.ini')

api_key = config.get('BINANCE', 'API_KEY', fallback=None)
api_secret = config.get('BINANCE', 'API_SECRET', fallback=None)

# Initialize Binance exchange
binance = ccxt.binance({
    'apiKey': api_key,
    'secret': api_secret,
    'enableRateLimit': True,  # Enforce rate limits
})

# Synchronize time with Binance server
binance.load_time_difference()

# Function to perform internal transfer
def internal_transfer(amount, transfer_type):
    """
    Perform internal transfer between spot and futures wallets.

    :param amount: Amount to transfer
    :param transfer_type: Transfer direction (1 = Spot to Futures, 2 = Futures to Spot)
    """
    try:
        # Binance API requires specific parameters
        response = binance.sapi_post_futures_transfer({
            'asset': 'USDT',          # Asset to transfer
            'amount': amount,         # Amount to transfer
            'type': transfer_type     # 1 = Spot to Futures, 2 = Futures to Spot
        })
        print(f"Transfer successful: {response}")
    except Exception as e:
        print(f"Error during transfer: {e}")

# Determine transfer amount and direction
def calculate_and_transfer():
    # Fetch balances for spot and futures wallets
    spot_balance = binance.fetch_balance()['total']['USDT']
    futures_balance = binance.fetch_balance(params={'type': 'future'})['total']['USDT']

    # Calculate average balance
    total_balance = spot_balance + futures_balance
    average_balance = total_balance / 2
    print(f"Target balanced = ",average_balance)

    # Determine transfer amount and direction
    if spot_balance > average_balance:
        transfer_amount = spot_balance - average_balance
        transfer_type = 1  # Spot to Futures
    elif futures_balance > average_balance:
        transfer_amount = futures_balance - average_balance
        transfer_type = 2  # Futures to Spot
    else:
        print("No transfer needed. Spot and Futures wallets are balanced.")
        return

    # Perform the transfer
    print(f"Initiating transfer of {transfer_amount:.6f} USDT.")
    internal_transfer(amount=transfer_amount, transfer_type=transfer_type)

# Execute the transfer process
calculate_and_transfer()

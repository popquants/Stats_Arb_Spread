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

def fetch_portfolio_free_above_one():
    try:
        # Initialize portfolios for spot and futures
        spot_portfolio = []
        futures_portfolio = []

        # Fetch spot balances
        spot_balance = binance.fetch_balance()
        for asset_data in spot_balance['info']['balances']:
            asset = asset_data['asset']
            free_balance = float(asset_data['free'])  # Convert to float for comparison

            # Filter assets with free balance > 1
            if free_balance > 1:
                spot_portfolio.append({
                    'asset': asset,
                    'free_balance': free_balance,
                    'wallet': 'spot'
                })

        # Fetch futures balances
        futures_balance = binance.fetch_balance(params={'type': 'future'})
        for asset, details in futures_balance['total'].items():
            free_balance = float(details)  # Futures balance is already aggregated as 'total'

            # Filter assets with free balance > 1
            if free_balance > 1:
                futures_portfolio.append({
                    'asset': asset,
                    'free_balance': free_balance,
                    'wallet': 'futures'
                })

        # Combine spot and futures portfolios
        combined_portfolio = spot_portfolio + futures_portfolio

        # Display the combined portfolio in a readable format
        for item in combined_portfolio:
            print(f"Wallet: {item['wallet']}, Asset: {item['asset']}, Free Balance: {item['free_balance']:.8f}")

        return combined_portfolio

    except Exception as e:
        print(f"Error fetching and filtering portfolio: {e}")
        return []

# Fetch and process the portfolio
portfolio = fetch_portfolio_free_above_one()

"""
Script to fetch 1h OHLCV data for USDT Perpetual pairs from Binance Futures,
then pivot and compute pct_change for each pair's close price.
Also, before processing, it checks that each trading pair's volume in the last 24 hours is > 100,000,000.
"""

import configparser
import ccxt
import pandas as pd
from datetime import datetime, timedelta
import time
import math


class BinanceFuturesDataFetcher:
    def __init__(self, days=20, timeframe='1h'):
        """
        :param days:       Number of days of data to fetch.
        :param timeframe:  Timeframe for OHLCV (e.g. '1h', '4h', '1d').
        """
        self.days = days
        self.timeframe = timeframe
        self.api_key = None
        self.api_secret = None
        self.binance_futures = None
        self.usdt_pairs = []
        self.final_df = None
        self.wide_df = None
        self.trading_pct_change = None

        # Load config, init ccxt, load markets
        self._load_config()
        self._init_binance_futures()
        self._load_usdt_perpetual_pairs()

    def _load_config(self):
        """Load API keys from config.ini."""
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.api_key = config.get('BINANCE', 'API_KEY', fallback=None)
        self.api_secret = config.get('BINANCE', 'API_SECRET', fallback=None)

    def _init_binance_futures(self):
        """Initialize the ccxt binance futures client."""
        self.binance_futures = ccxt.binance({
            'apiKey': self.api_key,
            'secret': self.api_secret,
            'options': {
                'defaultType': 'future'
            }
        })
        self.binance_futures.load_markets()

    def _load_usdt_perpetual_pairs(self):
        """Filter out USDT-margined PERPETUAL pairs from the loaded markets."""
        for symbol, market in self.binance_futures.markets.items():
            if (market.get('quote') == 'USDT'
                and market.get('info', {}).get('contractType') == 'PERPETUAL'):
                self.usdt_pairs.append(symbol)
        print(f"Found {len(self.usdt_pairs)} USDT PERPETUAL pairs.")

    def fetch_and_process_data(self):
        """Main method to fetch OHLCV data, pivot, and compute pct_change."""
        if not self.usdt_pairs:
            print("No USDT PERPETUAL pairs found. Exiting.")
            return None, None

        # Calculate 'since' for the desired days ago.
        since = int((datetime.utcnow() - timedelta(days=self.days)).timestamp() * 1000)

        all_data = []
        total_pairs = len(self.usdt_pairs)
        print(f"Fetching {self.timeframe} data for the last {self.days} days...")

        for idx, symbol in enumerate(self.usdt_pairs, start=1):
            # Progress bar (text-based)
            progress_percent = (idx / total_pairs) * 100
            bar_filled = "#" * math.ceil(progress_percent / 2)
            bar_unfilled = "-" * (50 - len(bar_filled))
            print(f"[{bar_filled}{bar_unfilled}] {progress_percent:.2f}% | "
                  f"Fetching: {symbol} ({idx}/{total_pairs})")

            try:
                ohlcv = self.binance_futures.fetch_ohlcv(
                    symbol, timeframe=self.timeframe, since=since
                )
                df = pd.DataFrame(
                    ohlcv,
                    columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
                )
                # Keep only timestamp, close, and volume.
                df = df[['timestamp', 'close', 'volume']]
                df['symbol'] = symbol
                all_data.append(df)

                # Respect rate limit.
                time.sleep(self.binance_futures.rateLimit / 1000)

            except ccxt.BadSymbol:
                print(f"Skipping {symbol} - BadSymbol error (invalid pair).")
            except ccxt.NetworkError as ne:
                print(f"Skipping {symbol} - Network error: {ne}")
            except ccxt.ExchangeError as ee:
                print(f"Skipping {symbol} - Exchange error: {ee}")
            except Exception as e:
                print(f"Skipping {symbol} - Unexpected error: {e}")

        if not all_data:
            print("No data was fetched at all.")
            return None, None

        # Concatenate fetched data into a single long-format DataFrame.
        self.final_df = pd.concat(all_data, ignore_index=True)
        print("\nLong-format DataFrame (timestamp, close, volume, symbol):")
        print(self.final_df.head())

        # --- Volume Check (Per Trading Pair) ---
        # Convert 'timestamp' (ms) to datetime (tz-naive).
        self.final_df['timestamp'] = pd.to_datetime(self.final_df['timestamp'], unit='ms')
        # Create cutoff timestamp for the last 24 hours (tz-naive).
        cutoff = (pd.Timestamp.utcnow() - pd.Timedelta(hours=24)).tz_localize(None)
        last_24 = self.final_df[self.final_df['timestamp'] >= cutoff]
        # Group by symbol and sum the volume.
        vol_by_symbol = last_24.groupby('symbol')['volume'].sum()
        print("\nVolume by trading pair in the last 24 hours:")
        print(vol_by_symbol)
        # Only keep symbols with volume > 100,000,000.
        valid_symbols = vol_by_symbol[vol_by_symbol > 200000000].index.tolist()
        if not valid_symbols:
            print("No trading pair has volume > 100,000,000 in the last 24 hours. Exiting.")
            return None, None
        else:
            print(f"\nTrading pairs meeting the volume criteria: {valid_symbols}")
        # Filter final_df to only include rows for valid trading pairs.
        self.final_df = self.final_df[self.final_df['symbol'].isin(valid_symbols)]

        # --- Pivoting to Wide Format ---
        self.wide_df = self.final_df.pivot(index='timestamp', columns='symbol', values='close')
        self.wide_df.sort_index(inplace=True)
        self.wide_df.columns.name = None

        # Convert index 'timestamp' from index to column for further processing.
        self.wide_df.reset_index(inplace=True)
        # Create a new 'datetime' column (example: adjust by adding 7 hours)
        self.wide_df['datetime'] = pd.to_datetime(self.wide_df['timestamp']) + pd.Timedelta(hours=7)
        # Insert 'datetime' after 'timestamp'
        self.wide_df.insert(
            self.wide_df.columns.get_loc('timestamp') + 1,
            'datetime',
            self.wide_df.pop('datetime')
        )
        # Set 'datetime' as index and drop 'timestamp'
        self.wide_df.set_index('datetime', inplace=True)
        self.wide_df.drop(columns=['timestamp'], inplace=True)

        # --- Remove ':USDT' from column names (if present) ---
        self.wide_df.rename(
            columns=lambda c: c.replace(':USDT', ''),
            inplace=True
        )

        print("\nWide-format DataFrame with 'datetime' as index (timestamp dropped):")
        print(self.wide_df.head())

        # Compute percentage change for each pair's close price.
        self.trading_pct_change = self.wide_df.pct_change()
        self.trading_pct_change.dropna(how='all', inplace=True)
        self.trading_pct_change.rename(
            columns=lambda c: c.replace(':USDT', ''),
            inplace=True
        )

        # Return the final wide DataFrame and the trading_pct_change DataFrame.
        return self.wide_df, self.trading_pct_change


if __name__ == '__main__':
    fetcher = BinanceFuturesDataFetcher(days=20, timeframe='1h')
    wide_df, trading_pct_change = fetcher.fetch_and_process_data()

    if wide_df is not None and trading_pct_change is not None:
        print("\nTrading pct_change:")
        print(trading_pct_change.head())
        
        # Save the dataframes to CSV files (including index which is datetime)
        wide_df.to_csv("wide_df.csv", index=True)
        trading_pct_change.to_csv("trading_pct_change.csv", index=True)
        print("Data saved to 'wide_df.csv' and 'trading_pct_change.csv'.")

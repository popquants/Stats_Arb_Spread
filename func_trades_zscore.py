"""
Statistical Arbitrage Trading System
This script implements a mean reversion strategy using cointegration analysis.
It runs hourly analysis and manages positions based on z-score conditions.
"""

import time
import pandas as pd
import numpy as np
from datetime import datetime
import ccxt
import configparser
import logging
import os
from func_get_prices import BinanceFuturesDataFetcher
from func_cointegration import get_cointegrated_pairs
from func_rank_zscore import calculate_zscore, process_pairs
from func_mean_halflife import compute_mean_and_halflife

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trading.log'),
        logging.StreamHandler()
    ]
)

# Trading parameters
POSITION_SIZE = 50  # USD per side
LEVERAGE = 10
STOP_LOSS_PERCENT = 0.03  # 3% of portfolio
ZSCORE_THRESHOLD = -1.5
TP_BUFFER = 0.0002  # 0.02% buffer for take profit

def check_balance(binance_futures):
    """Check if we have sufficient balance for trading."""
    try:
        balance = binance_futures.fetch_balance()
        free_usdt = float(balance['free']['USDT'])
        required = POSITION_SIZE * 2  # For both long and short positions
        
        if free_usdt < required:
            logging.warning(f"Insufficient balance. Required: {required} USDT, Available: {free_usdt} USDT")
            return False
        return True
    except Exception as e:
        logging.error(f"Error checking balance: {str(e)}")
        return False

def set_leverage(binance_futures, symbol, leverage):
    """Set leverage for a symbol."""
    try:
        binance_futures.set_leverage(leverage, symbol)
        logging.info(f"Successfully set leverage to {leverage}x for {symbol}")
        return True
    except Exception as e:
        logging.error(f"Error setting leverage for {symbol}: {str(e)}")
        return False

def open_positions(binance_futures, long_pair, short_pair, hedge_ratio):
    """Open long and short positions."""
    try:
        # Set leverage for both pairs
        set_leverage(binance_futures, long_pair, LEVERAGE)
        set_leverage(binance_futures, short_pair, LEVERAGE)
        
        # Get current prices
        long_price = binance_futures.fetch_ticker(long_pair)['last']
        short_price = binance_futures.fetch_ticker(short_pair)['last']
        
        # Calculate quantities
        long_quantity = POSITION_SIZE / long_price
        short_quantity = POSITION_SIZE / short_price
        
        # Place orders
        long_order = binance_futures.create_order(
            symbol=long_pair,
            type='market',
            side='buy',
            amount=long_quantity
        )
        
        short_order = binance_futures.create_order(
            symbol=short_pair,
            type='market',
            side='sell',
            amount=short_quantity
        )
        
        # Log
        message = f"Opened positions:\nLong: {long_pair} ({long_quantity:.4f})\nShort: {short_pair} ({short_quantity:.4f})"
        logging.info(message)
        
        return [long_order, short_order]
    except Exception as e:
        logging.error(f"Error opening positions: {str(e)}")
        return None

def close_positions(binance_futures, orders):
    """Close open positions."""
    try:
        for order in orders:
            symbol = order['symbol']
            side = 'sell' if order['side'] == 'buy' else 'buy'
            amount = order['filled']
            
            close_order = binance_futures.create_order(
                symbol=symbol,
                type='market',
                side=side,
                amount=amount
            )
            
            logging.info(f"Closed position for {symbol}: {close_order['id']}")
        
        message = "All positions closed"
        logging.info(message)
    except Exception as e:
        logging.error(f"Error closing positions: {str(e)}")

def check_positions(binance_futures, orders, mean_zscore):
    """Check if positions should be closed based on TP/SL conditions."""
    try:
        # Get current positions
        positions = binance_futures.fetch_positions()
        
        # Calculate current PnL
        total_pnl = 0
        for position in positions:
            if float(position['contracts']) > 0:
                total_pnl += float(position['unrealizedPnl'])
        
        logging.info(f"Current total PnL: {total_pnl}")
        
        # Check stop loss
        if total_pnl <= -STOP_LOSS_PERCENT * POSITION_SIZE * 2:
            logging.info(f"Stop loss triggered. PnL: {total_pnl}")
            return True
        
        # Get current z-score
        current_zscore = get_current_zscore(binance_futures, orders[0]['symbol'], orders[1]['symbol'], mean_zscore)
        if current_zscore is None:
            logging.error("Failed to get current z-score")
            return False
            
        logging.info(f"Current z-score: {current_zscore:.4f}")
        logging.info(f"Mean z-score: {mean_zscore:.4f}")
        logging.info(f"Z-score difference: {abs(current_zscore - mean_zscore):.4f}")
        logging.info(f"Take profit buffer: {TP_BUFFER}")
        
        # Check take profit
        if abs(current_zscore - mean_zscore) <= TP_BUFFER:
            logging.info("Take profit triggered")
            return True
        
        return False
    except Exception as e:
        logging.error(f"Error checking positions: {str(e)}")
        return False

def get_current_zscore(binance_futures, pair1, pair2, hedge_ratio):
    """Get current z-score for the pair."""
    try:
        # Get recent OHLCV data
        ohlcv1 = binance_futures.fetch_ohlcv(pair1, timeframe='1h', limit=20)
        ohlcv2 = binance_futures.fetch_ohlcv(pair2, timeframe='1h', limit=20)
        
        # Calculate spread
        spread = [candle1[4] - hedge_ratio * candle2[4] 
                 for candle1, candle2 in zip(ohlcv1, ohlcv2)]
        
        # Calculate z-score
        zscore = calculate_zscore(spread)
        return zscore[-1]
    except Exception as e:
        logging.error(f"Error getting current z-score: {str(e)}")
        return None

def prepare_data_files():
    """Prepare necessary data files for trading."""
    try:
        # Get price data
        fetcher = BinanceFuturesDataFetcher(days=20, timeframe='1h')
        wide_df, trading_pct_change = fetcher.fetch_and_process_data()
        
        if wide_df is None or trading_pct_change is None:
            logging.error("Failed to fetch price data")
            return False
        
        # Save wide_df to CSV
        wide_df.to_csv('wide_df.csv')
        logging.info("Saved wide_df.csv")
        
        # Get cointegrated pairs
        cointegrated_pairs = get_cointegrated_pairs(wide_df)
        if cointegrated_pairs.empty:
            logging.error("No cointegrated pairs found")
            return False
        
        # Save cointegrated pairs
        cointegrated_pairs.to_csv('df_cointegrated_pairs.csv', index=False)
        logging.info("Saved df_cointegrated_pairs.csv")
        
        # Process pairs and calculate z-scores
        process_pairs()
        
        # Get mean z-scores
        try:
            mean_zscore_df = compute_mean_and_halflife('df_top50_coint.csv')
            if mean_zscore_df.empty:
                logging.error("Failed to compute mean z-scores")
                return False
            
            # Save mean z-scores
            mean_zscore_df.to_csv('df_mean_halflife.csv', index=False)
            logging.info("Saved df_mean_halflife.csv")
        except Exception as e:
            logging.error(f"Error computing mean z-scores: {str(e)}")
            return False
        
        return True
    except Exception as e:
        logging.error(f"Error preparing data files: {str(e)}")
        return False

def main():
    """Main trading function."""
    try:
        logging.info("Starting trading system...")
        
        # Initialize Binance Futures
        config = configparser.ConfigParser()
        config.read('config.ini')
        api_key = config.get('BINANCE', 'API_KEY')
        api_secret = config.get('BINANCE', 'API_SECRET')
        
        binance_futures = ccxt.binanceusdm({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True
        })
        logging.info("Successfully initialized Binance Futures")
        
        # Prepare data files first
        if not prepare_data_files():
            logging.error("Failed to prepare data files. Exiting.")
            return
        
        current_orders = None
        logging.info("Entering main trading loop...")
        
        while True:
            current_time = datetime.now()
            logging.info(f"Current time: {current_time}")
            
            # Check if it's time for hourly analysis (x:01)
            if current_time.minute == 1:
                logging.info("Starting hourly analysis")
                
                # Prepare data files
                if not prepare_data_files():
                    logging.error("Failed to prepare data files")
                    time.sleep(60)
                    continue
                
                # Get cointegrated pairs
                cointegrated_pairs = pd.read_csv('df_cointegrated_pairs.csv')
                if cointegrated_pairs.empty:
                    logging.info("No cointegrated pairs found")
                    time.sleep(60)
                    continue
                
                # Get z-scores from df_top50_coint.csv (for entry signals)
                zscore_df = pd.read_csv('df_top50_coint.csv')
                if zscore_df.empty:
                    logging.error("No z-score data found")
                    time.sleep(60)
                    continue
                
                # Get mean z-scores from df_mean_halflife.csv (for take profit)
                mean_zscore_df = pd.read_csv('df_mean_halflife.csv')
                if mean_zscore_df.empty:
                    logging.error("No mean z-score data found")
                    time.sleep(60)
                    continue
                
                # Find best trading opportunity
                best_pair = None
                best_zscore = float('inf')
                best_mean_zscore = None
                
                logging.info("Searching for trading opportunities...")
                for _, row in cointegrated_pairs.iterrows():
                    pair_name = f"{row['sym_1']}:{row['sym_2']}"
                    zscore_col = f"{pair_name}_zscore"
                    mean_zscore_col = f"{pair_name}_mean_zscore"
                    
                    if zscore_col in zscore_df.columns and mean_zscore_col in mean_zscore_df.columns:
                        zscore = zscore_df[zscore_col].iloc[-1]  # Get the last value
                        mean_zscore = mean_zscore_df[mean_zscore_col].iloc[-1]  # Get the last mean value
                        logging.info(f"Pair {row['sym_1']}/{row['sym_2']} has z-score: {zscore:.4f}, mean z-score: {mean_zscore:.4f}")
                        if zscore < best_zscore:
                            best_zscore = zscore
                            best_mean_zscore = mean_zscore
                            best_pair = (row['sym_1'], row['sym_2'], row['hedge_ratio'])
                
                logging.info(f"Best z-score found: {best_zscore:.4f}")
                logging.info(f"Best mean z-score: {best_mean_zscore:.4f}")
                logging.info(f"Z-score threshold: {ZSCORE_THRESHOLD}")
                
                # Check if we have a valid opportunity
                if best_zscore < ZSCORE_THRESHOLD and best_pair is not None:
                    long_pair, short_pair, hedge_ratio = best_pair
                    logging.info(f"Found trading opportunity: {long_pair}/{short_pair} with z-score {best_zscore:.4f}")
                    
                    # Check balance and open positions
                    if check_balance(binance_futures):
                        logging.info("Balance check passed, attempting to open positions...")
                        current_orders = open_positions(binance_futures, long_pair, short_pair, hedge_ratio)
                        if current_orders is None:
                            logging.error("Failed to open positions")
                    else:
                        logging.warning("Insufficient balance for trading")
                else:
                    logging.info("No valid trading opportunity found")
                
                time.sleep(60)
            
            # If we have open positions, check them every 3 minutes
            elif current_orders is not None:
                logging.info("Checking open positions...")
                if check_positions(binance_futures, current_orders, best_mean_zscore):
                    close_positions(binance_futures, current_orders)
                    current_orders = None
                time.sleep(180)  # Check every 3 minutes
            
            else:
                time.sleep(60)  # Wait 1 minute before next check
    
    except Exception as e:
        logging.error(f"Main loop error: {str(e)}")
        logging.error(f"Error details: {str(e.__class__.__name__)}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    logging.info("Script started")
    main() 
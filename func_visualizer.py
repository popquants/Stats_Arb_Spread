import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from datetime import datetime
import time
import ccxt
import configparser
import logging
from func_trades_zscore import get_current_zscore, check_balance

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('visualizer.log'),
        logging.StreamHandler()
    ]
)

# Initialize Binance Futures
def init_binance():
    config = configparser.ConfigParser()
    config.read('config.ini')
    api_key = config.get('BINANCE', 'API_KEY')
    api_secret = config.get('BINANCE', 'API_SECRET')
    
    return ccxt.binanceusdm({
        'apiKey': api_key,
        'secret': api_secret,
        'enableRateLimit': True
    })

def get_current_trading_pair(binance_futures):
    """Get the current trading pair with open positions from Binance Futures"""
    try:
        # First try to get from trading.log
        try:
            with open('trading.log', 'r') as f:
                log_lines = f.readlines()
                # Look for the most recent "Found trading opportunity" line
                for line in reversed(log_lines):
                    if "Found trading opportunity:" in line:
                        # Extract pair from log line
                        pair_info = line.split("Found trading opportunity: ")[1].split(" with z-score")[0]
                        # Split by / and format pairs with USDT
                        pairs = pair_info.split('/')
                        pair1 = f"{pairs[0]}/USDT"  # PENGU/USDT
                        pair2 = f"{pairs[2]}/USDT"  # WLD/USDT
                        logging.info(f"Found trading pair from log: {pair1}/{pair2}")
                        
                        # Get hedge ratio from cointegrated pairs
                        cointegrated_pairs = pd.read_csv('df_cointegrated_pairs.csv')
                        for _, row in cointegrated_pairs.iterrows():
                            if (row['sym_1'] == pair1 and row['sym_2'] == pair2) or \
                               (row['sym_1'] == pair2 and row['sym_2'] == pair1):
                                return row['sym_1'], row['sym_2'], row['hedge_ratio']
        except Exception as e:
            logging.warning(f"Could not read from trading.log: {str(e)}")
        
        # If no pair found in log, try to get from open positions
        positions = binance_futures.fetch_positions()
        open_positions = [pos for pos in positions if float(pos['contracts']) != 0]
        
        if len(open_positions) >= 2:
            symbols = [pos['symbol'] for pos in open_positions]
            cointegrated_pairs = pd.read_csv('df_cointegrated_pairs.csv')
            for _, row in cointegrated_pairs.iterrows():
                if row['sym_1'] in symbols and row['sym_2'] in symbols:
                    logging.info(f"Found open positions for pair: {row['sym_1']}/{row['sym_2']}")
                    return row['sym_1'], row['sym_2'], row['hedge_ratio']
                elif row['sym_2'] in symbols and row['sym_1'] in symbols:
                    logging.info(f"Found open positions for pair: {row['sym_2']}/{row['sym_1']}")
                    return row['sym_2'], row['sym_1'], row['hedge_ratio']
        
        logging.info("No matching open positions found")
        return None, None, None
    except Exception as e:
        logging.error(f"Error reading trading pair: {str(e)}")
        return None, None, None

def get_balance_info(binance_futures):
    try:
        # Get total balance
        balance = binance_futures.fetch_balance()
        total_usdt = balance['USDT']['total']
        
        # Get unrealized PnL from positions
        positions = binance_futures.fetch_positions()
        unrealized_pnl = sum(float(pos['unrealizedPnl']) for pos in positions)
        
        return total_usdt, unrealized_pnl
    except Exception as e:
        logging.error(f"Error fetching balance: {str(e)}")
        return None, None

def create_zscore_chart():
    """Create an empty chart with the required traces"""
    fig = go.Figure()
    
    # Add traces for current z-score and mean z-score
    fig.add_trace(go.Scatter(
        y=[],
        mode='lines+markers',
        name='Current Z-Score',
        line=dict(color='blue')
    ))
    
    fig.add_trace(go.Scatter(
        y=[],
        mode='lines+markers',
        name='Mean Z-Score',
        line=dict(color='red')
    ))
    
    # Add entry level line (will be hidden by default)
    fig.add_hline(
        y=0,
        line_dash="dash",
        line_color="green",
        annotation_text="Entry Level",
        annotation_position="right",
        visible=False
    )
    
    # Update layout
    fig.update_layout(
        title='Z-Score Monitoring',
        yaxis_title='Z-Score',
        showlegend=True,
        height=400
    )
    
    return fig

def create_pnl_chart():
    """Create an empty chart for PnL"""
    fig = go.Figure()
    
    # Add trace for PnL
    fig.add_trace(go.Scatter(
        y=[],
        mode='lines+markers',
        name='Unrealized PnL',
        line=dict(color='green')
    ))
    
    # Update layout
    fig.update_layout(
        title='Unrealized PnL Monitoring',
        yaxis_title='USDT',
        showlegend=True,
        height=400
    )
    
    return fig

def get_current_zscore_and_mean(binance_futures, pair1, pair2, hedge_ratio):
    """Get current z-score and mean z-score for the pair."""
    try:
        # Get current z-score
        current_zscore = get_current_zscore(binance_futures, pair1, pair2, hedge_ratio)
        
        # Get mean z-score from df_mean_halflife.csv
        mean_zscore_df = pd.read_csv('df_mean_halflife.csv')
        pair_name = f"{pair1}:{pair2}"
        mean_zscore_col = f"{pair_name}_mean_zscore"
        
        if mean_zscore_col in mean_zscore_df.columns:
            mean_zscore = mean_zscore_df[mean_zscore_col].iloc[-1]
        else:
            mean_zscore = 0.0
            
        return current_zscore, mean_zscore
    except Exception as e:
        logging.error(f"Error getting z-scores: {str(e)}")
        return None, None

def close_all_positions(binance_futures):
    """Close all open positions"""
    try:
        # Get all positions
        positions = binance_futures.fetch_positions()
        open_positions = [pos for pos in positions if float(pos['contracts']) != 0]
        
        if not open_positions:
            return "No open positions to close"
        
        # Close each position
        for pos in open_positions:
            # Clean up symbol format (remove :USDT suffix if present)
            symbol = pos['symbol'].replace(':USDT', '')
            contracts = abs(float(pos['contracts']))
            
            # Determine position direction and order side
            if float(pos['contracts']) > 0:  # Long position
                close_side = 'sell'  # Need to sell to close long
            else:  # Short position
                close_side = 'buy'   # Need to buy to close short
            
            try:
                # Create market order to close position
                binance_futures.create_order(
                    symbol=symbol,
                    type='market',
                    side=close_side,
                    amount=contracts
                )
                logging.info(f"Created market order to close {close_side} position: {symbol} {contracts} contracts")
            except Exception as e:
                logging.error(f"Error closing position {symbol}: {str(e)}")
                return f"Error closing position {symbol}: {str(e)}"
        
        return "Successfully created orders to close all positions"
    except Exception as e:
        logging.error(f"Error in close_all_positions: {str(e)}")
        return f"Error closing positions: {str(e)}"

def main():
    st.set_page_config(page_title="Trading Dashboard", layout="wide")
    st.title("Trading Dashboard")
    
    # Initialize Binance
    binance_futures = init_binance()
    
    # Initialize session state
    if 'entry_zscore' not in st.session_state:
        st.session_state.entry_zscore = None
    if 'chart' not in st.session_state:
        st.session_state.chart = create_zscore_chart()
    if 'pnl_chart' not in st.session_state:
        st.session_state.pnl_chart = create_pnl_chart()
    if 'current_zscore_data' not in st.session_state:
        st.session_state.current_zscore_data = []
    if 'mean_zscore_data' not in st.session_state:
        st.session_state.mean_zscore_data = []
    if 'pnl_data' not in st.session_state:
        st.session_state.pnl_data = []
    if 'last_update' not in st.session_state:
        st.session_state.last_update = None
    if 'last_pair' not in st.session_state:
        st.session_state.last_pair = None
    if 'close_message' not in st.session_state:
        st.session_state.close_message = None
    
    # Create placeholders for metrics and charts
    metric_placeholder = st.empty()
    info_placeholder = st.empty()
    chart_placeholder = st.empty()
    pnl_chart_placeholder = st.empty()
    
    # Add Close All Positions button
    if st.button("Close All Positions", type="primary"):
        with st.spinner("Closing all positions..."):
            st.session_state.close_message = close_all_positions(binance_futures)
            st.write(st.session_state.close_message)
    
    while True:
        try:
            # Get current time
            current_time = datetime.now()
            
            # Get balance information
            total_usdt, unrealized_pnl = get_balance_info(binance_futures)
            if total_usdt is not None:
                with metric_placeholder.container():
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Total USDT Balance", f"{total_usdt:.2f} USDT")
                    with col2:
                        st.metric("Unrealized PnL", f"{unrealized_pnl:.2f} USDT")
            
            # Get current trading pair
            pair1, pair2, hedge_ratio = get_current_trading_pair(binance_futures)
            if pair1 is None or pair2 is None or hedge_ratio is None:
                with info_placeholder.container():
                    st.info("No open positions found")
                time.sleep(60)
                continue
            
            # Update info in place
            with info_placeholder.container():
                st.write(f"Last Updated: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                st.write(f"Current Trading Pair: {pair1}/{pair2} (Hedge Ratio: {hedge_ratio:.4f})")
            
            # Get current z-scores
            current_zscore, mean_zscore = get_current_zscore_and_mean(binance_futures, pair1, pair2, hedge_ratio)
            if current_zscore is None or mean_zscore is None:
                st.error("Failed to get z-scores")
                time.sleep(60)
                continue
            
            # Update z-score data
            st.session_state.current_zscore_data.append(current_zscore)
            st.session_state.mean_zscore_data.append(mean_zscore)
            st.session_state.pnl_data.append(unrealized_pnl)
            
            # Keep only last 100 data points
            if len(st.session_state.current_zscore_data) > 100:
                st.session_state.current_zscore_data = st.session_state.current_zscore_data[-100:]
                st.session_state.mean_zscore_data = st.session_state.mean_zscore_data[-100:]
                st.session_state.pnl_data = st.session_state.pnl_data[-100:]
            
            # Update z-score chart
            st.session_state.chart.update_traces(
                y=st.session_state.current_zscore_data,
                selector=dict(name='Current Z-Score')
            )
            st.session_state.chart.update_traces(
                y=st.session_state.mean_zscore_data,
                selector=dict(name='Mean Z-Score')
            )
            
            # Update PnL chart
            st.session_state.pnl_chart.update_traces(
                y=st.session_state.pnl_data,
                selector=dict(name='Unrealized PnL')
            )
            
            # Update entry level if needed
            if current_zscore < -1.5 and st.session_state.entry_zscore is None:
                st.session_state.entry_zscore = current_zscore
                st.session_state.chart.update_annotations(
                    visible=True,
                    y=current_zscore
                )
            elif current_zscore > -0.5 and st.session_state.entry_zscore is not None:
                st.session_state.entry_zscore = None
                st.session_state.chart.update_annotations(
                    visible=False
                )
            
            # Display updated charts
            chart_placeholder.plotly_chart(st.session_state.chart, use_container_width=True)
            pnl_chart_placeholder.plotly_chart(st.session_state.pnl_chart, use_container_width=True)
            
            # Wait for 1 minute before next update
            time.sleep(60)
            
        except Exception as e:
            logging.error(f"Error in main loop: {str(e)}")
            st.error(f"Error occurred: {str(e)}")
            time.sleep(60)  # Wait 1 minute before retrying

if __name__ == "__main__":
    main() 
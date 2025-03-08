import ccxt
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import statsmodels.api as sm

# Initialize CCXT Binance client
exchange = ccxt.binance({
    'enableRateLimit': True
})

# Function to get bid and ask data
def get_bid_ask(pair, timeframe, since):
    ohlcv = exchange.fetch_ohlcv(pair, timeframe, since=since)
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['bid'] = df['low']  # Using low as bid
    df['ask'] = df['high'] # Using high as ask
    return df[['timestamp', 'bid', 'ask', 'close']]

# Function to calculate spread
def calculate_spread(df):
    df['spread'] = df['ask'] - df['bid']
    return df

# Function to plot data
def plot_data(cumulative_returns, spreads, raw_spread, zscore_spread, pairs):
    plt.figure(figsize=(12, 12))

    # Plot cumulative returns
    plt.subplot(3, 1, 1)
    for pair, cum_return in cumulative_returns.items():
        timestamps = cum_return.index
        plt.plot(timestamps, cum_return.values, label=f'{pair} Cumulative Returns')
    plt.title('Cumulative Returns of Close Prices')
    plt.xlabel('Datetime')
    plt.ylabel('Cumulative Returns')
    plt.legend()

    # Plot raw spread
    plt.subplot(3, 1, 2)
    plt.plot(raw_spread['timestamp'], raw_spread['spread'], label='Raw Spread')
    plt.title(f'Raw Spread: {pairs[0]} - {pairs[1]}')
    plt.xlabel('Datetime')
    plt.ylabel('Spread')
    plt.legend()

    # Plot z-score of raw spread
    plt.subplot(3, 1, 3)
    plt.plot(zscore_spread['timestamp'], zscore_spread['zscore'], label='Z-Score of Raw Spread')
    plt.title(f'Z-Score of Raw Spread: {pairs[0]} - {pairs[1]}')
    plt.xlabel('Datetime')
    plt.ylabel('Z-Score')
    plt.legend()

    plt.tight_layout()
    plt.show()

# Get user input for trading pairs
print("Enter the two trading pairs you want to analyze (e.g., 'BTCUSDT', 'ETHUSDT')")
pair1 = input("Enter the Long trading pair: ").strip().upper()
pair2 = input("Enter the Short trading pair: ").strip().upper()
pairs = [pair1, pair2]

# Set timeframe and start date
timeframe = '1d'
since = int(datetime(2024, 1, 1).timestamp() * 1000)  # From January 2024

cumulative_returns = {}
spreads = {}

for pair in pairs:
    # Get bid and ask data
    df = get_bid_ask(pair, timeframe, since)

    # Calculate spread
    df = calculate_spread(df)

    # Store spread data
    spreads[pair] = df

    # Calculate cumulative returns for close price
    pct_changes = df.set_index('timestamp')['close'].pct_change().dropna()
    cumulative_returns[pair] = (1 + pct_changes).cumprod() - 1

# Calculate hedge ratio using OLS regression
daily_prices = pd.merge(spreads[pair1][['timestamp', 'close']], spreads[pair2][['timestamp', 'close']], on='timestamp', suffixes=(f'_{pair1}', f'_{pair2}'))
X = daily_prices[f'close_{pair2}']
Y = daily_prices[f'close_{pair1}']
X = sm.add_constant(X)  # Add constant for intercept
ols_model = sm.OLS(Y, X).fit()
hedge_ratio = ols_model.params[f'close_{pair2}']
print(f"Hedge Ratio (β): {hedge_ratio}")
print(ols_model.summary())

# Calculate raw spread
raw_spread = pd.DataFrame()
raw_spread['timestamp'] = spreads[pair1]['timestamp']
raw_spread['spread'] = daily_prices[f'close_{pair1}'] - ols_model.params[0] * daily_prices[f'close_{pair2}']

# Calculate rolling z-score of raw spread with a window of 30
rolling_mean = raw_spread['spread'].rolling(window=30).mean()
rolling_std = raw_spread['spread'].rolling(window=30).std()
raw_spread['zscore'] = (raw_spread['spread'] - rolling_mean) / rolling_std

# Plot the data
plot_data(cumulative_returns, spreads, raw_spread, raw_spread, pairs)

import pandas as pd

class CorrelationCalculator:
    def __init__(self, threshold=0.8):
        """
        :param threshold: Only pairs with correlation > threshold are kept.
        """
        self.threshold = threshold

    def get_correlations(self, trading_pct_change):
        """
        Computes all pairwise correlations among columns in `trading_pct_change`,
        removes ':USDT' from symbol names, filters only pairs with correlation > threshold,
        sorts them descending by correlation, and returns a single-row DataFrame
        (columns = 'SymbolA:SymbolB', values = correlation).
        """
        # 1) Compute the correlation matrix.
        corr_matrix = trading_pct_change.corr()

        # 2) Flatten the upper triangle.
        pairs = []
        cols = corr_matrix.columns
        for i in range(len(cols)):
            for j in range(i + 1, len(cols)):
                col1, col2 = cols[i], cols[j]
                # Remove ":USDT" from the names.
                col1_clean = col1.replace(":USDT", "")
                col2_clean = col2.replace(":USDT", "")
                pair_name = f"{col1_clean}:{col2_clean}"
                cor_value = corr_matrix.iloc[i, j]
                pairs.append((pair_name, cor_value))

        # 3) Filter only pairs with correlation > threshold.
        filtered_pairs = [(pn, cv) for (pn, cv) in pairs if cv > self.threshold]

        # 4) Sort descending by correlation value.
        filtered_pairs.sort(key=lambda x: x[1], reverse=True)

        # 5) Build dictionary for the final single-row DataFrame.
        correlation_dict = {}
        print("=== Pairs with correlation > {:.2f} (sorted high -> low) ===".format(self.threshold))
        for pair_name, cor_value in filtered_pairs:
            correlation_dict[pair_name] = cor_value
            print(f"{pair_name} => correlation = {cor_value:.2f}")

        # 6) Create and return a single-row DataFrame.
        correlation_df = pd.DataFrame([correlation_dict])
        return correlation_df


if __name__ == '__main__':
    # Read trading_pct_change from CSV (ensure the file exists in the working directory)
    trading_pct_change = pd.read_csv("trading_pct_change.csv", index_col=0)

    # Create an instance of the CorrelationCalculator with a threshold of 0.9.
    calculator = CorrelationCalculator(threshold=0.8)
    corr_df = calculator.get_correlations(trading_pct_change)

    # Save the resulting correlation DataFrame to a CSV file.
    corr_df.to_csv("corr_df.csv", index=False)
    print("Correlation DataFrame saved to 'corr_df.csv'.")

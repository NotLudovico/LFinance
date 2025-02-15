import numpy as np
import os
import json
import matplotlib.pyplot as plt


def compute_cross_correlation(series1, series2, max_lag):
    """
    Compute the cross-correlation of two time series with specified lag range.
    """
    if not isinstance(series1, (list, np.ndarray)) or not isinstance(
        series2, (list, np.ndarray)
    ):
        raise ValueError("Both inputs must be lists or numpy arrays.")

    series1 = np.asarray(series1)
    series2 = np.asarray(series2)

    if len(series1) != len(series2):
        shortest = len(series1)
        if len(series2) < shortest:
            shortest = len(series2)
        series1 = series1[-shortest:]
        series2 = series2[-shortest:]

    lags = np.arange(-max_lag, max_lag + 1)
    correlations = []

    for lag in lags:
        if lag < 0:
            shifted_series1 = series1[:lag]
            shifted_series2 = series2[-lag:]
        elif lag > 0:
            shifted_series1 = series1[lag:]
            shifted_series2 = series2[:-lag]
        else:
            shifted_series1 = series1
            shifted_series2 = series2

        # Compute correlation and handle potential NaN values
        correlation = np.corrcoef(shifted_series1, shifted_series2)[0, 1]
        correlations.append(correlation if not np.isnan(correlation) else 0)

    return lags, np.array(correlations)


def load_commodities_from_directory(directory_path):
    """
    Load JSON data from files in the specified directory.
    """
    commodities = []
    for filename in os.listdir(directory_path):
        if filename.endswith(".json"):
            file_path = os.path.join(directory_path, filename)
            try:
                with open(file_path, "r") as json_file:
                    data = json.load(json_file)
                    # Validate required keys
                    if "prices" in data and "company_name" in data:
                        commodities.append(data)
                    else:
                        print(f"Skipping {filename}: Missing required keys.")
            except Exception as e:
                print(f"Error loading {filename}: {e}")
    return commodities


def normalize_series(series):
    """
    Normalize a time series using min-max normalization to scale data to [0, 1].
    """
    series = np.asarray(series)
    return (series - np.min(series)) / (np.max(series) - np.min(series))


def plot_correlation_with_prices(
    lags, correlations, series1, series2, company_1, company_2, output_dir
):
    """
    Plot and save the cross-correlation and the price series shifted by the lag
    that maximizes correlation if the max correlation > 0.75.
    """
    max_correlation = np.max(correlations)
    max_lag = lags[np.argmax(correlations)]

    if max_correlation > 0.8:
        # Normalize series
        series1 = normalize_series(series1)
        series2 = normalize_series(series2)

        # Plot correlation
        fig, ax = plt.subplots(2, 1, figsize=(12, 8))

        ax[0].plot(lags, correlations, marker="o", linestyle="-")
        ax[0].set_title(
            f"Cross-Correlation: {company_1} vs {company_2} (Max: {max_correlation:.2f} at Lag {max_lag})"
        )
        ax[0].set_xlabel("Lag")
        ax[0].set_ylabel("Correlation")
        ax[0].grid(True)

        # Shift and plot prices
        if max_lag < 0:
            shifted_series1 = series1[:max_lag]
            shifted_series2 = series2[-max_lag:]
        elif max_lag > 0:
            shifted_series1 = series1[max_lag:]
            shifted_series2 = series2[:-max_lag]
        else:
            shifted_series1 = series1
            shifted_series2 = series2

        ax[1].plot(shifted_series1, label=f"{company_1} (Shifted)", alpha=0.75)
        ax[1].plot(shifted_series2, label=f"{company_2} (Shifted)", alpha=0.75)
        ax[1].set_title(f"Aligned Price Series: {company_1} vs {company_2}")
        ax[1].set_xlabel("Time (Weeks)")
        ax[1].set_ylabel("Price")
        ax[1].legend()
        ax[1].grid(True)

        plt.tight_layout()

        # Save the plot
        output_path = os.path.join(
            output_dir, f"{company_1}_vs_{company_2}_with_prices.png"
        )
        plt.savefig(output_path)
        plt.close()
        print(f"Plot saved: {output_path}")


# Main script
directory_path = "./data"  # Make this configurable
output_directory = "./plots"  # Directory to save plots
os.makedirs(output_directory, exist_ok=True)

commodities = load_commodities_from_directory(directory_path)

# Compute cross-correlations and generate plots
for i, comm1 in enumerate(commodities):
    for j, comm2 in enumerate(commodities):
        if i < j:  # Avoid duplicate calculations (comm1, comm2) == (comm2, comm1)
            lags, corrs = compute_cross_correlation(comm1["prices"], comm2["prices"], 6)

            if len(corrs) < 2:
                break

            # Generate and save the plot if max correlation > 0.75
            plot_correlation_with_prices(
                lags,
                corrs,
                comm1["prices"],
                comm2["prices"],
                comm1["company_name"],
                comm2["company_name"],
                output_directory,
            )

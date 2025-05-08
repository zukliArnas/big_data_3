import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

from logger_config import get_logger

logger = get_logger("task.log")

def fetch_filtered_data() -> pd.DataFrame:
    """Connect to MongoDB and fetch filtered AIS data with MMSI and Timestamp."""
    clean_data = "ais_clean"
    logger.info("Connecting to MongoDB and fetching filtered data ...")

    client = MongoClient("mongodb://localhost:27017")
    db = client["ais"]
    collection = db[clean_data]

    cursor = collection.find({}, {"MMSI": 1, "Timestamp": 1, "_id": 0})
    df = pd.DataFrame(list(cursor))
    client.close()

    logger.info(f"Fetched {len(df)} records from {clean_data}")
    return df

def calculate_delta_t(df: pd.DataFrame) -> pd.DataFrame:
    """Parse timestamps and compute delta_t in milliseconds and seconds."""
    logger.info("Parsing timestamps and calculating delta_t in ms/seconds ...")
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], format="%d/%m/%Y %H:%M:%S")
    df = df.sort_values(["MMSI", "Timestamp"])
    df["delta_t_ms"] = df.groupby("MMSI")["Timestamp"].diff().dt.total_seconds() * 1000
    df = df.dropna(subset=["delta_t_ms"])
    df["delta_t_s"] = df["delta_t_ms"] / 1000

    logger.info(f"Calculated delta_t for {len(df)} records.")
    return df

def plot_histogram(df: pd.DataFrame) -> None:
    """Generate and save a histogram of delta_t values (log-scaled Y, annotated)."""
    logger.info("Generating enhanced histogram of delta_t values...")
    df_filtered = df[df["delta_t_s"] < 100]  # Keep delta_t below 100s

    plt.figure(figsize=(14, 7))
    counts, bins, patches = plt.hist(
        df_filtered["delta_t_s"],
        bins=100,
        edgecolor="black",
        log=True
    )

    # Add mean and median lines
    mean_dt = df_filtered["delta_t_s"].mean()
    median_dt = df_filtered["delta_t_s"].median()

    plt.axvline(mean_dt, color='green', linestyle='--', linewidth=2, label=f"Mean: {mean_dt:.1f}s")
    plt.axvline(median_dt, color='orange', linestyle='--', linewidth=2, label=f"Median: {median_dt:.1f}s")

    # Add known AIS interval annotations
    for t in [2, 10, 30, 60]:
        plt.axvline(x=t, color='red', linestyle='--', linewidth=1)
        plt.text(t + 0.5, 1e3, f"{t}s", rotation=90, color='red', fontsize=10)

    plt.xlabel("Delta t (seconds)", fontsize=14, fontweight="bold")
    plt.ylabel("Frequency (log scale)", fontsize=14, fontweight="bold")
    plt.title(
        "Histogram of Time Differences Between Vessel Data Points (Zoomed < 100s, Log Y)",
        fontsize=15, fontweight="bold"
    )
    plt.legend()

    ax = plt.gca()
    ax.tick_params(axis='both', labelsize=12)
    for label in ax.get_xticklabels() + ax.get_yticklabels():
        label.set_fontweight("bold")

    ax.grid(True, linewidth=0.8, linestyle="--")
    plt.xlim(0, 100)
    plt.tight_layout()
    plt.savefig("delta_t_histogram_final.png")
    plt.show()

    logger.info("Final histogram saved as delta_t_histogram_final.png")

def main() -> None:
    """Run delta_t analysis and plot generation."""
    df = fetch_filtered_data()
    df = calculate_delta_t(df)
    plot_histogram(df)

if __name__ == "__main__":
    main()

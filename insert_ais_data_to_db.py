import math
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import pandas as pd
import pymongo
from pymongo.errors import PyMongoError
from tqdm import tqdm

from logger_config import get_logger

CSV_PATH = "/Users/arnas/big_data_3/big_data_3/ais_dataset/aisdk-2025-04-20/aisdk-2025-04-20.csv"
NUM_THREADS = 2
CHUNK_SIZE = 5000

logger = get_logger("task.log")


def insert_chunk(chunk_df: pd.DataFrame) -> bool:
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["ais"]
    collection = db["records"]
    records = chunk_df.to_dict(orient="records")

    for attempt in range(3):
        try:
            collection.insert_many(records, ordered=False)
            logger.info(f"Inserted chunk of {len(records)}")
            client.close()
            return True
        except PyMongoError as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(2 * (attempt + 1))

    logger.error("Final insert attempt failed. Giving up.")
    client.close()
    return False


def prepare_dataframe(csv_path: str) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        logger.error(f"Failed to load CSV: {e}")
        return None

    df.rename(columns={"# Timestamp": "Timestamp"}, inplace=True)
    df.dropna(subset=["MMSI", "Timestamp", "Latitude", "Longitude"], inplace=True)

    logger.info(f"Total rows after cleaning: {len(df)}")
    return df


def main() -> bool:
    start_time = time.time()

    logger.info("Loading and cleaning data")
    df = pd.read_csv(CSV_PATH)
    if df is None or df.empty:
        logger.error("No data to insert.")
        return False

    total_rows = len(df)
    chunks = math.ceil(total_rows / CHUNK_SIZE)

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = []
        for i in tqdm(range(chunks), desc="Inserting chunks"):
            start = i * CHUNK_SIZE
            end = min(start + CHUNK_SIZE, total_rows)
            chunk_df = df.iloc[start:end]
            futures.append(executor.submit(insert_chunk, chunk_df))
            time.sleep(0.1)

        for f in futures:
            if not f.result():
                logger.warning("One or more chunks failed to insert.")

    elapsed = time.time() - start_time
    logger.info(f"Data insertion complete. Total execution time: {elapsed:.2f} seconds.")
    return True


if __name__ == "__main__":
    success = main()
    if success:
        print("Data was inserted successfully.")
    else:
        print("Data insertion failed.")

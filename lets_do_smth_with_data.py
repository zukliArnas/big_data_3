import math
import time

from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm

from logger_config import get_logger


CSV_PATH = "/Users/arnas/big_data_3/big_data_3/ais_dataset/aisdk-2025-04-20/aisdk-2025-04-20.csv"
CHUNK_SIZE = 5000
NUM_THREADS = 2

logger = get_logger("task.log")

def insert_chunk(chunk_df):
    client = MongoClient("mongodb://localhost:27017")
    db = client["ais"]
    collection = db["records"]

    try:
        records = chunk_df.to_dict(orient="records")
        collection.insert_many(records)
        logger.info(f"Inserted chunk of {len(records)}")
    except Exception as e:
        logger.error(f"Chunk failed: {e}")
        time.sleep(2)  # Wait before retrying
    finally:
        client.close()

def main():
    logger.info("Loading and cleaning data")
    df = pd.read_csv(CSV_PATH)

    logger.info(f"Total rows: {len(df)}")
    logger.info(f"Doing cleaning")

    df.rename(columns={"# Timestamp": "Timestamp"}, inplace=True)
    df = df[["MMSI", "Timestamp", "Latitude", "Longitude", "SOG", "COG"]]
    df = df.dropna(subset=["MMSI", "Timestamp", "Latitude", "Longitude"])

    total_rows = len(df)
    logger.info(f"After cleaning left rows: {total_rows}")

    chunks = math.ceil(total_rows / CHUNK_SIZE)
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as ex:
        futures = []
        for i in tqdm(range(chunks), desc="Inserting chunks"):
            start = i * CHUNK_SIZE
            end = min(start + CHUNK_SIZE, total_rows)
            chunk_df = df.iloc[start:end]
            futures.append(ex.submit(insert_chunk, chunk_df))

        for f in futures:
            f.result()

    print("Data was inserted succesfully.")

if __name__ == "__main__":
    start_time = time.time()
    main()
    total_time = time.time() - start_time
    logger.info(f"Total execution time: {total_time:.2f} seconds.")
    print(f"Total execution time: {total_time:.2f} seconds.")
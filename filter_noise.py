import math
import time
import pymongo
import numpy as np

from multiprocessing import Pool, cpu_count
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from tqdm import tqdm
from typing import List, Any

from logger_config import get_logger


REQUIRED_FIELDS = [
    "MMSI", "Navigational status", "Latitude", "Longitude",
    "ROT", "SOG", "COG", "Heading"
]

BATCH_SIZE = 5000
CHUNK_SIZE = 500
NUM_PROCESSES = max(cpu_count() - 1, 1)
BULK_WRITE_SIZE = 1000

logger = get_logger("task.log")


def get_client() -> MongoClient:
    """Create and return a configured MongoDB client."""
    return pymongo.MongoClient(
        "mongodb://localhost:27017",
        maxPoolSize=200,
        connectTimeoutMS=30000,
        socketTimeoutMS=45000,
        waitQueueTimeoutMS=30000,
        retryWrites=True,
        w=1
    )

def init_worker() -> None:
    """Worker initializer to create a global MongoDB client per process."""
    global client
    client = get_client()


def is_valid_entry(entry: dict) -> bool:
    """Check whether an entry is valid based on required fields."""
    for field in REQUIRED_FIELDS:
        if field not in entry:
            return False
        value = entry[field]
        if value in [None, "", -1] or (isinstance(value, float) and math.isnan(value)):
            return False
    return True


def process_mmsi_chunk(mmsi_chunk: List[Any]) -> int:
    """Filter and insert valid records for a chunk of MMSI values."""
    global client
    db = client["ais"]
    raw = db["records"]
    clean_records = []
    inserted_count = 0

    for mmsi in mmsi_chunk:
        cursor = raw.find(
            {"MMSI": mmsi},
            projection={field: 1 for field in REQUIRED_FIELDS + ["_id", "Timestamp"]},
            batch_size=BATCH_SIZE
        )
        entries = list(cursor)
        if len(entries) < 100:
            logger.debug(f"Skipping MMSI {mmsi} with only {len(entries)} entries")
            continue

        valid_entries = []
        for entry in entries:
            if is_valid_entry(entry):
                valid_entries.append(entry)


        if valid_entries:
            clean_records.extend(valid_entries)
            inserted_count += len(valid_entries)


        if len(clean_records) >= BULK_WRITE_SIZE:
            if clean_records:
                db["ais_clean"].insert_many(clean_records, ordered=False)
                clean_records = []

    if clean_records:
        db["ais_clean"].insert_many(clean_records, ordered=False)

    return inserted_count


def chunk_list(lst: List[Any], n: int) -> List[List[Any]]:
    """Split a list into n roughly equal chunks"""
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]


def prepare_database() -> List[Any]:
    """Drop old clean data, index raw data, and fetch shuffled MMSIs."""
    client = get_client()
    db = client["ais"]

    logger.info("Cleaning old data...")
    db["ais_clean"].drop()

    # Create index on MMSI in raw collection if it doesn't exist
    logger.info("Ensuring indexes on raw data...")
    if "MMSI_1" not in db["records"].index_information():
        db["records"].create_index([("MMSI", pymongo.ASCENDING)])

    logger.info("Fetching unique MMSIs...")
    mmsi_list = db["records"].distinct("MMSI")

    # Shuffle the MMSI list for better load distribution
    np.random.shuffle(mmsi_list)

    client.close()
    return mmsi_list


def main() -> bool:
    start_time = time.time()

    mmsi_list = prepare_database()
    logger.info(f"Found {len(mmsi_list)} MMSIs. Starting parallel processing...")

    mmsi_chunks = chunk_list(mmsi_list, NUM_PROCESSES * 4)

    # Process in parallel
    with Pool(processes=NUM_PROCESSES, initializer=init_worker) as pool:
        results = list(tqdm(
            pool.imap_unordered(process_mmsi_chunk, mmsi_chunks),
            total=len(mmsi_chunks),
            desc="Processing MMSI chunks"
        ))

    total_inserted = sum(results)
    logger.info(f"Filtering complete. Total valid entries inserted: {total_inserted}")

    # Create indexes after data is inserted
    client = get_client()
    db = client["ais"]

    logger.info("Creating indexes on clean data...")
    db["ais_clean"].create_index([("MMSI", pymongo.ASCENDING)])
    db["ais_clean"].create_index([("MMSI", pymongo.ASCENDING), ("Timestamp", pymongo.ASCENDING)])
    client.close()

    elapsed_time = time.time() - start_time
    logger.info(f"Clean data is ready in 'ais_clean' collection. Total time: {elapsed_time:.2f} seconds")
    return True


if __name__ == "__main__":
    success = main()
    print("Data filtering completed successfully." if success else "Filtering failed.")

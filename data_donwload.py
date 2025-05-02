import requests
from tqdm import tqdm
from zipfile import ZipFile
import time
import os

from logger_config import get_logger


url = "https://web.ais.dk/aisdata/aisdk-2025-04-20.zip"
output_dir = "ais_dataset"
os.makedirs(output_dir, exist_ok=True)



logger = get_logger("task.log")

def download_the_dataset(url: str, output_path: str, force: bool) -> str | None:
    if output_path is None:
        output_path = os.path.basename(url) or "downloaded_file"

    if os.path.exists(output_path) and not force:
        logger.info(f"File '{output_path}' already exists. Skipping download.")
        return output_path

    logger.info(f"Starting download from: {url}")

    response = requests.get(url=url, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))

    try:        # Using stream=True, because it is benificial for big files. It doesn't save everything into RAMS, but devides into chunks
        with open(output_path, "wb") as file, tqdm(
            total=total_size, unit="B", unit_scale=True, desc=output_path,
            ) as progress_bar:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)
                    progress_bar.update(len(chunk))  # Update progress bar
    except requests.exceptions.RequestException as e:
        logger.error(f"Download failed: {e}")
        return None

    logger.info(f"File successfully downloaded: {output_path}")
    return output_path


def unzip_the_file(file_path: str) -> str | None:
    if not file_path.endswith(".zip"):
        logger.warning(f"{file_path} is not a ZIP. Skipping.")
        return None

    extract_folder = file_path[:-4]

    if os.path.exists(extract_folder):
        logger.info(f"Folder '{extract_folder}' already exists. Skipping extraction.")
        return extract_folder

    try:
        with ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)
    except Exception as e:
        logger.error(f"Failed to extract '{file_path}': {e}")
        return None

    logger.info(f"Successfully extracted '{file_path}' to '{extract_folder}'.")
    return extract_folder


def find_csv_file(extract_folder: str) -> str | None:
    for file_name in os.listdir(extract_folder):
        if file_name.endswith('.csv'):
            csv_file_path = os.path.join(extract_folder, file_name)
            logger.info(f"Found CSV file: {csv_file_path}")
            return csv_file_path
    logger.warning("No CSV file found in the extracted folder.")
    return None

def main():
    zip_path = os.path.join(output_dir, os.path.basename(url))
    download_the_dataset(url, zip_path, force=False)
    extracted_folder = unzip_the_file(zip_path)
    if extracted_folder:
        find_csv_file(extracted_folder)


if __name__ == "__main__":
    start_time = time.time()
    main()
    total_time = time.time() - start_time
    logger.info(f"Total execution time: {total_time:.2f} seconds.")
    print(f"Total execution time: {total_time:.2f} seconds.")
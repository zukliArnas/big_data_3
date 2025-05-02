import requests
import zipfile
import io
import os

url = "https://web.ais.dk/aisdata/aisdk-2025-04-20.zip"
output_dir = "ais_dataset"
os.makedirs(output_dir, exist_ok=True)

zip_path = os.path.join(output_dir, "ais_data.zip")

print("Downloading dataset...")
try:
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    print("Download complete. Extracting files...")

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(output_dir)

    print(f"✅ Dataset extracted to: {output_dir}")

except Exception as e:
    print(f"❌ Error: {e}")

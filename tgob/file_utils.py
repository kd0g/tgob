import os
import hashlib
import requests
import gzip
import logging
from .file_index import add_to_index

def calculate_md5(file_path):
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        logging.error(f"Failed to calculate MD5 for {file_path}: {e}")
        return None

def verify_gzip(file_path):
    try:
        with gzip.open(file_path, 'rb') as f:
            while f.read(1024):
                pass
        return True
    except Exception as e:
        logging.error(f"Failed to verify {file_path}: {e}")
        return False

def verify_file(file_path, file_name, index):
    if not os.path.exists(file_path):
        return False

    file_size = os.path.getsize(file_path)
    file_md5 = calculate_md5(file_path)
    if file_name in index and index[file_name] == (file_md5, file_size):
        logging.info(f"File {file_path} is already indexed and valid.")
        return True

    logging.info(f"File {file_path} is not indexed or invalid. Verifying with gzip...")
    if verify_gzip(file_path):
        add_to_index(file_name, file_md5, file_size, index)
        logging.info(f"File {file_path} is valid and indexed.")
    else:
        logging.warning(f"File {file_path} is corrupted and will be deleted.")
        os.remove(file_path)
        return False

    return True

def download_file(url, file_path):
    try:
        with requests.get(url, stream=True, timeout=30) as response:
            response.raise_for_status()
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        logging.info(f"Successfully downloaded {file_path}")
    except requests.RequestException as e:
        logging.error(f"Failed to download {url}: {e}")
    except Exception as e:
        logging.error(f"Error occurred while downloading {url}: {e}")
        traceback.print_exc()

import os
import re
import requests
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from tgob.file_utils import verify_file, download_file
from tqdm import tqdm

def download_and_verify(ticker, file_name, index, progress_bar, basedir=''):
    url = f"https://public.bybit.com/trading/{ticker}/{file_name}"
    dir_name = f"raw{ticker.lower()}"
    dir_name = os.path.join(basedir, dir_name)
    file_path = os.path.join(dir_name, file_name)

    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    if verify_file(file_path, file_name, index):
        progress_bar.update(1)
        return

    while True:
        download_file(url, file_path)
        if verify_file(file_path, file_name, index):
            break

    progress_bar.update(1)

def download_files_for_ticker(ticker, index, basedir=''):
    url = f"https://public.bybit.com/trading/{ticker}/"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        links = re.findall(r'href="([^"]*.gz)"', response.text)
        if not links:
            logging.info(f"No links found for ticker {ticker}")
            return

        with tqdm(total=len(links), desc=f"Downloading {ticker}", unit="file") as progress_bar:
            with ThreadPoolExecutor(max_workers=32) as executor:
                futures = [executor.submit(download_and_verify, ticker, file_name, index, progress_bar, basedir) for file_name in links]
                for future in as_completed(futures):
                    try:
                        future.result(timeout=60)
                    except TimeoutError:
                        logging.error("A task has timed out.")
                    except Exception as e:
                        logging.error(f"Error occurred: {e}")
                        traceback.print_exc()

    except requests.RequestException as e:
        logging.error(f"Failed to retrieve links for ticker {ticker}: {e}")

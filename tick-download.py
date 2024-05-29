import logging
from downloader.bybit import download_files_for_ticker
from tgob.file_index import read_index

def download_all_tickers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    TICKERS = ["BTCUSDT", "XRPUSDT", "ETHUSDT", "SOLUSDT", "1000PEPEUSDT", "DOGEUSDT", "1000BONKUSDT"]

    index = read_index()

    for ticker in TICKERS:
        download_files_for_ticker(ticker, index, basedir='backdata/tickdata', WORKERS=16)

def main():
    download_all_tickers()

if __name__ == "__main__":
    main()

import glob
import os
import pandas as pd
import logging
import traceback
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed, TimeoutError

WORKERS = 4

def process_file(file_path, dst_dir, timeframes):
    file_name = os.path.basename(file_path)
    ticker = file_name.split('202')[0].lower()

    for timeframe in timeframes:
        df = pd.read_csv(file_path)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
        df.set_index('datetime', inplace=True)
        ohlcv = df.resample(timeframe).agg({
            'price': ['first', 'max', 'min', 'last'],
            'size': 'sum'
        }).dropna()

        output_dir = dst_dir + f'/{timeframe}/{ticker}'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        try:
            output_file = output_dir + f'/{file_name}'
            ohlcv.to_csv(output_file, index=True, header=False, compression='gzip')
        except:
            os.remove(output_file)

def main():
    src_dir = 'backdata/tickdata'
    src_patt = src_dir + '/*/*'
    source = glob.glob(src_patt)
    source_files = {os.path.basename(file_path) for file_path in source}

    files_to_process = set()
    timeframes = ['1s', '15s', '1min', '5min', '15min', '1h', '4h']

    for timeframe in timeframes:
        dst_dir = 'backdata/candledata'
        dst_patt = dst_dir + f'/{timeframe}/*/*'
        destination = glob.glob(dst_patt)

        # source와 destination 파일 이름만 추출 (경로 제거)
        destination_files = {os.path.basename(file_path) for file_path in destination}
        missing_files = source_files - destination_files
        files_to_process.update(missing_files)

    # 경로를 포함한 파일 처리
    with tqdm(total=len(files_to_process), desc=f"Candle Data for {timeframes}", unit="file") as progress_bar:
        with ProcessPoolExecutor(max_workers=WORKERS) as executor:
            futures = [executor.submit(process_file, file_path, dst_dir, timeframes) for file_path in source if os.path.basename(file_path) in files_to_process]
            for future in as_completed(futures):
                try:
                    future.result(timeout=60)
                except TimeoutError:
                    logging.error("A task has timed out.")
                except Exception as e:
                    logging.error(f"Error occurred: {e}")
                    traceback.print_exc()
                finally:
                    progress_bar.update(1)

if __name__ == "__main__":
    main()

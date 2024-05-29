import os
import logging

INDEX_FILE = "/tmp/file_index.txt"

def read_index():
    index = {}
    if os.path.exists(INDEX_FILE):
        try:
            with open(INDEX_FILE, 'r') as index_file:
                for line in index_file:
                    file_name, file_md5, file_size = line.strip().split('\t')
                    index[file_name] = (file_md5, int(file_size))
        except Exception as e:
            logging.error(f"Failed to read index file: {e}")
    return index

def add_to_index(file_name, file_md5, file_size, index):
    try:
        index[file_name] = (file_md5, file_size)
        with open(INDEX_FILE, 'w') as index_file:
            for name, (md5, size) in index.items():
                index_file.write(f"{name}\t{md5}\t{size}\n")
    except Exception as e:
        logging.error(f"Failed to add {file_name} to index: {e}")

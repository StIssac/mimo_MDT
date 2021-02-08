from zipfile import ZipFile
from pathlib import Path
from multiprocessing import Pool
import logging
import time
import pandas as pd
from io import StringIO
import os
COLUMNS_INDEX_LIST = list(range(0, 3))+list(range(5, 37))+list(range(65, 69))+list(range(74, 78))
__input_dir__ = './MDT'
__out_dir__ = './new_csv'


def process_csv(raw_data, name):
    lat_long = raw_data.iloc[:, [14, 16]]
    row_number = lat_long.notnull().any(axis=1)
    data = raw_data.iloc[:, COLUMNS_INDEX_LIST]
    data = data[row_number]
    new_name = __out_dir__ + '/' + name
    data.to_csv(new_name, index=False)


def _extract_file(filename):
    archive = ZipFile(filename, 'r')
    for name in archive.namelist():
        file = ZipFile(archive.open(name, 'r'))
        for csv_name in file.namelist():
            if csv_name.endswith('.csv'):
                csv_file = file.read(csv_name)
                csv_file = csv_file.decode('unicode_escape')
                raw_data = StringIO(csv_file)
                raw_data = pd.read_csv(raw_data, sep=',', skiprows=1, index_col=False)
                process_csv(raw_data, csv_name)


logging.basicConfig(format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG)

if __name__ == '__main__':
    if not os.path.exists(__out_dir__):
        os.mkdir(__out_dir__)
    start = time.time()
    pool = Pool()
    process = []
    path_ = Path(__input_dir__)
    zip_files = list(path_.glob('*.zip'))
    pool.map(_extract_file, zip_files)
    pool.close()
    pool.join()
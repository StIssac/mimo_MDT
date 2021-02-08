from pathlib import Path
import pandas as pd
from multiprocessing import Pool
__out_dir__ = './new_csv'
COLUMNS_INDEX_LIST = list(range(0, 3))+list(range(5, 37))+list(range(65, 69))+list(range(74, 78))


def process_csv(name):
    raw_data = pd.read_csv(name, skiprows=1, index_col=False)
    lat_long = raw_data.iloc[:, [14, 16]]
    row_number = lat_long.notnull().any(axis=1)
    data = raw_data.iloc[:, COLUMNS_INDEX_LIST]
    data = data[row_number]
    new_name = __out_dir__ + '/' + name.name
    data.to_csv(new_name, index=False)


if __name__ == '__main__':
    Path(__out_dir__).mkdir(parents=True,exist_ok=True)
    path_ = Path('./csv')
    csv_files = list(path_.glob('*.csv'))
    pool = Pool()
    pool.map(process_csv, csv_files)
    pool.close()
    pool.join()
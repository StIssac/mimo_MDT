from pathlib import Path
import pandas as pd
__out_dir__ = './new_csv'
COLUMNS_INDEX_LIST = list(range(0,3))+list(range(5,37))+list(range(65,69))+list(range(74,78))


Path(__out_dir__).mkdir(parents=True,exist_ok=True)
path_ = Path('./csv')
csv_files = list(path_.glob('*.csv'))

for filename in csv_files:
    raw_data = pd.read_csv(filename, skiprows=1, index_col=False)
    Lat_Long = raw_data.iloc[:, [14, 16]]
    row_number = Lat_Long.notnull().any(axis=1)
    data = raw_data.iloc[:, COLUMNS_INDEX_LIST]
    data = data[row_number]
    name = __out_dir__+'/'+filename.name
    data.to_csv(name,index=False)


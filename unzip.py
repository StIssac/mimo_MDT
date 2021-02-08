from zipfile import ZipFile
from pathlib import Path
__input_dir__ = './MDT'


path_ = Path(__input_dir__)
zip_files = list(path_.glob('*.zip'))
for filename in zip_files:
    with ZipFile(filename, 'r') as zipObj:
        zipObj.extractall('./first_unzip')

path_ = Path('./first_unzip')
zip_files = list(path_.glob('*.zip'))
for filename in zip_files:
    with ZipFile(filename, 'r') as zipObj:
        zipObj.extractall('./csv')

from testUnzip import f1, f2, f3
from pathlib import Path
__input_dir__ = './MDT'


path_ = Path(__input_dir__)
zip_files = list(path_.glob('*.zip'))
for filename in zip_files:
    f2(filename, './first_unzip')

path_ = Path('./first_unzip')
zip_files = list(path_.glob('*.zip'))
for filename in zip_files:
    f2(filename, './csv')
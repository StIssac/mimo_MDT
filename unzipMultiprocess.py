from zipfile import ZipFile
from pathlib import Path
from multiprocessing import Process, Queue
__input_dir__ = './MDT'


def _extract_file(name, dir_):
    with ZipFile(name, 'r') as zipObj:
        zipObj.extractall(dir_)


queue = Queue()
process = [];
path_ = Path(__input_dir__)
zip_files = list(path_.glob('*.zip'))
for filename in zip_files:
    p = Process(target=_extract_file(filename, './first_unzip'))



path_ = Path('./first_unzip')
zip_files = list(path_.glob('*.zip'))
for filename in zip_files:
    p = Process(target=_extract_file(filename, './csv'))

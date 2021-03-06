from zipfile import ZipFile
from pathlib import Path
from multiprocessing import Process, Pool, set_start_method
import logging
import time
__input_dir__ = './MDT'


def _extract_file(num):
    global g_zip_files
    with ZipFile(g_zip_files(num), 'r') as zipObj:
        zipObj.extractall('./first_unzip')


def _extract_file2(num):
    global g_unzip_files
    with ZipFile(g_unzip_files(num), 'r') as zipObj:
        zipObj.extractall('./csv')


logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG)

if __name__ == '__main__':
    start = time.time()
    pool = Pool()
    process = []
    path_ = Path(__input_dir__)
    g_zip_files = list(path_.glob('*.zip'))
    length = list(range(len(g_zip_files)))
    pool.map(_extract_file, length)
    pool.close()
    pool.join()

    pool2 = Pool()
    process2 = []
    path_ = Path('./first_unzip')
    g_unzip_files = list(path_.glob('*.zip'))
    length2 = list(range(len(g_unzip_files)))
    pool2.map(_extract_file2, length2)
    pool2.close()
    pool2.join()


from zipfile import ZipFile
from pathlib import Path
from multiprocessing import Pool
import logging
import time
__input_dir__ = './MDT'


def _extract_file(name):
    logging.debug('unzipping file: %s' % name)
    with ZipFile(name, 'r') as zipObj:
        zipObj.extractall('./first_unzip')


def _extract_file2(name):
    logging.debug('unzipping file: %s' % name)
    with ZipFile(name, 'r') as zipObj:
        zipObj.extractall('./csv')


logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG)

if __name__ == '__main__':
    start = time.time()
    pool = Pool()
    process = []
    path_ = Path(__input_dir__)
    zip_files = list(path_.glob('*.zip'))
    pool.map(_extract_file, zip_files)
    pool.close()
    pool.join()

    pool2 = Pool()
    process2 = []
    path_ = Path('./first_unzip')
    zip_files = path_.glob('*.zip')
    pool2.map(_extract_file2, zip_files)
    pool2.close()
    pool2.join()


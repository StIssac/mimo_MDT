from zipfile import ZipFile
from pathlib import Path
from multiprocessing import Pool
import logging
import time
__input_dir__ = './MDT'


def _extract_file(filename):
    archive = ZipFile(filename, 'r')
    for name in archive.namelist():
        file = ZipFile(archive.open(name, 'r'))
        file.extractall('./csv')


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
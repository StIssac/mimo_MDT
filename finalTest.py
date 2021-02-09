from zipfile import ZipFile
from pathlib import Path
from multiprocessing import Pool
import logging
import pandas as pd
from io import StringIO
from time import time, strftime, localtime, sleep
import os
from configparser import ConfigParser
COLUMNS_INDEX_LIST = ['TimeStamp', 'MME Group ID', 'MME Code', 'MME UE S1AP ID',
                      'CellID', 'SC PCI', 'SC Freq', 'SCRSRP', 'SCRSRQ', 'PHR',
                      'SCTadv', 'SCAOA', 'Longitude', 'LatitudeSign', 'Latitude',
                      'AltitudeDirection', 'Altitude', 'Uncertainty', 'UncertaintySemiMajor',
                      'UncertaintySemiMinor', 'OrientationMajorAxis', 'UncertaintyAltitude',
                      'Confidence', 'NC1PCI', 'NC1Freq', 'NC1RSRP', 'NC1RSRQ', 'NC2PCI',
                      'NC2Freq', 'NC2RSRP', 'NC2RSRQ', 'NC3PCI', 'NC3Freq', 'NC3RSRP',
                      'NC3RSRQ', 'M4ULERAB1Volume', 'M4ULERAB2Volume', 'M4ULERAB3Volume',
                      'M4ULERAB4Volume', 'M4DLERAB1Volume', 'M4DLERAB2Volume', 'M4DLERAB3Volume',
                      'M4DLERAB4Volume', ]
input_dir = ''
out_dir = ''


def config():
    cp = ConfigParser()
    cp.read('config.cfg')
    global input_dir
    input_dir = cp['section1']['__input_dir__']
    global out_dir
    out_dir = cp['section1']['__out_dir__']


def process_csv(raw_data, name, dir_):
    lat_long = raw_data[['Longitude', 'Latitude']]
    row_number = lat_long.notnull().any(axis=1)
    data = raw_data[COLUMNS_INDEX_LIST]
    data = data[row_number]
    if dir_.endswith('/') or dir_.endswith('\\'):
        new_name = dir_ + 'processed/' + name
    else:
        new_name = dir_ + '/processed/' + name
    data.to_csv(new_name, index=False)


def _extract_file(input_str):
    filename = Path(input_str.split('|||')[0])
    dir_ = input_str.split('|||')[1]
    archive = ZipFile(filename, 'r')
    for name in archive.namelist():
        file = ZipFile(archive.open(name, 'r'))
        for csv_name in file.namelist():
            if csv_name.endswith('.csv'):
                start = time()
                csv_file = file.read(csv_name)
                csv_file = csv_file.decode('unicode_escape')
                raw_data = StringIO(csv_file)
                raw_data = pd.read_csv(raw_data, sep=',', skiprows=1, index_col=False)
                try:
                    process_csv(raw_data, csv_name, dir_)
                    end = time()
                    runtime_ = end - start
                    logging.debug('Processed file %s in %f' % (csv_name, runtime_))
                    print('Processed file %s in %f' % (csv_name, runtime_))
                except KeyError:
                    if dir_.endswith('/') or dir_.endswith('\\'):
                        new_name = dir_ + 'raw/' + csv_name
                    else:
                        new_name = dir_ + '/raw/' + csv_name
                    raw_data.to_csv(new_name, index=False)
                    logging.debug('Failed to process file %s ' % csv_name)
                    print('Failed to process file %s ' % csv_name)


def multiprocessing_start():
    pool = Pool()
    path_ = Path(input_dir)
    zip_files = list(path_.glob('*.zip'))
    # for file_name in zip_files:
    for i in range(0, len(zip_files)):
        zip_files[i] = str(zip_files[i])+'|||'+out_dir
    pool.map(_extract_file, zip_files)
    pool.close()
    pool.join()


def make_dirs():
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    if not os.path.exists(out_dir+'\\raw'):
        os.mkdir(out_dir+'\\raw')
    if not os.path.exists(out_dir+'\\processed'):
        os.mkdir(out_dir+'\\processed')


run_date = strftime("%Y-%m-%d %H'%M'%S", localtime())
logging.basicConfig(format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG,
                    filename='%s - MDT - running_log.log' % run_date)


if __name__ == '__main__':
    config()
    print('started')
    multiprocessing_start()
    print('finished')

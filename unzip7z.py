from pathlib import Path
import os
__input_dir__ = r'C:\Users\cmcc\PycharmProjects\pythonProject\MDT'


path_ = Path(__input_dir__)
zip_files = list(path_.glob('*.zip'))
for filename in zip_files:
    cmd = "7z.exe x \"%s\" -o%s -aoa" % (filename, os.getcwd()+'\\first_unzip')
    os.system(cmd)

path_ = Path('./first_unzip')
zip_files = list(path_.glob('*.zip'))
for filename in zip_files:
    cmd = "7z.exe x \"%s\" -o%s -aoa" % (filename, os.getcwd()+'\\csv')
    os.system(cmd)
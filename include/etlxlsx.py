import os
import logging
import platform
from datetime import datetime
import time

class XLSX_CSV_base:
    def __init__(self, c_fullpath: str):
        self.c_fullpath = c_fullpath

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, exc_tb):
        pass

    def get_req_tsn(self) -> str:
        is_dst = time.daylight and time.localtime().tm_isdst > 0
        utc_offset = - (time.altzone if is_dst else time.timezone)/3600
        # I'm aware about timezones with fraction of hour offset. But it is not relevent for the case
        return datetime.now().strftime("%Y%m%d%H%M%S") + 'UTC'+str(int(utc_offset))

    def move_column_inplace(self, df, col, pos: int):
        col = df.pop(col)
        df.insert(pos, col.name, col)

# will be overrided in sub-classes
    def save_file(self, fname: str):
        pass

    def copy_postgre(self):
        pass

    def extract_file_from_dir(self):
        lst_files = [ fi for fi in os.listdir(self.c_fullpath) if fi[-4:] == 'xlsx']
        print(f'current working dir = { os.getcwd() }') # /lessons

        if len(lst_files) > 0:
            if platform.system() == 'Windows':
                fname = f'{self.c_fullpath}\\{lst_files[0]}' # Windows
            else:
                fname = f'{self.c_fullpath}/{lst_files[0]}' # docker
            self.save_file(fname)
        else:
            logging.info(f'EXTRACT,E,001,{self.c_fullpath} has no xlsx files')
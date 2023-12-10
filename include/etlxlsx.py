import os
import logging
import platform
import pandas as pd
from datetime import datetime
import time

class XLSX_CSV_Base:

    def __init__(self, c_fullpath: str):
        """ 
         c_fullpath - directory with source xlsx-files
        """
        self.c_fullpath = c_fullpath
        self.data = pd.DataFrame()

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, exc_tb):
        pass

    def get_req_tsn(self) -> str:
        is_dst = time.daylight and time.localtime().tm_isdst > 0
        utc_offset = - (time.altzone if is_dst else time.timezone)/3600
        # I'm aware about timezones with fraction of hour offset.
        # But it is not relevent for the case
        return datetime.now().strftime("%Y%m%d%H%M%S") + 'UTC'+str(int(utc_offset))

    def move_column_inplace(self, df, col, pos: int):
        """ in the dataset df the method moves col to position pos """
        col = df.pop(col)
        df.insert(pos, col.name, col)

# will be overrided in sub-classes
    def etl(self, fname: str) -> int:
        res = self.read_excel(fname)
        if res == 0: 
           res = self.transform_data(fname)
           if res == 0:
              res = self.save_csv(fname)
        return res

    def read_excel(self, fname: str) -> int:
        return 0

    def transform_data(self, fname: str) -> int:
        return 0

    def save_csv(self, fname: str) -> int:
        return 0

    def get_csv_filename(self, fname: str) -> str:
        """ cut xlsx extention adn add csv"""
        return fname[:-4] + 'csv'

    def extract_file_from_dir(self) -> int:
        lst_files = [ fi for fi in os.listdir(self.c_fullpath) if fi[-4:] == 'xlsx']
        # print(f'current working dir = { os.getcwd() }') # /lessons

        if len(lst_files) > 0:
            if platform.system() == 'Windows':
                fname_src = f'{self.c_fullpath}\\{lst_files[0]}' # Windows
            else:
                fname_src = f'{self.c_fullpath}/{lst_files[0]}' # docker
            res = self.etl(fname_src)           
        else:
            logging.info(f'EXTRACT,E,001,{self.c_fullpath} has no xlsx files')
            res = 1
        return res
import os
import logging
import platform
import pandas as pd
from datetime import datetime
import time

class XLSX_CSV_Base:

    def __init__(self, c_fullpath: str, fcount: int = 1):
        """ 
         c_fullpath - directory with source xlsx-files
         fcount - number of xlsx-files to be processed
        """
        self.c_fullpath = c_fullpath
        self.data = pd.DataFrame()
        self.fname_xlsx = ""
        self.fcount = fcount
        self.lst_files_sel =[]

    @property
    def fname_csv(self): return self.get_csv_filename(self.fname_xlsx)

    @property
    def error_cnt(self): return sum(1 for x in self.lst_files_sel if x.get("res") > 0 )

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

    def etl(self) -> int:
        res = self.read_excel(self.fname_xlsx)
        if res == 0: 
           res = self.transform_data(self.fname_xlsx)
           if res == 0:
              res = self.save_csv(self.fname_xlsx)
        return res

# will be overrided in sub-classes
    def read_excel(self, fname: str) -> int:
        return 0

# will be overrided in sub-classes
    def transform_data(self, fname: str) -> int:
        return 0

# will be overrided in sub-classes
    def save_csv(self, fname: str) -> int:
        return 0

    def get_csv_filename(self, fname: str) -> str:
        """ cut xlsx extention adn add csv"""
        return fname[:-4] + 'csv'

    def extract_files_from_dir(self) -> []:
        lst_files = [ fi for fi in os.listdir(self.c_fullpath) if fi[-4:] == 'xlsx']
        # print(f'current working dir = { os.getcwd() }') # /lessons

        self.lst_files_sel = []
        for i, element in enumerate(lst_files):
            if i < self.fcount:
                self.lst_files_sel.append(dict(fname=element, res = '' )) 
        if len(self.lst_files_sel) > 0:
            for li in self.lst_files_sel:            
                if platform.system() == 'Windows':
                    self.fname_xlsx  = f'{self.c_fullpath}\\{li.get("fname")}' # Windows
                else:
                    self.fname_xlsx  = f'{self.c_fullpath}/{li.get("fname")}' # docker
                res = self.etl() 
                li.update({"res": res })
            return self.lst_files_sel  
        else:
            logging.info(f'EXTRACT,E,001,{self.c_fullpath} has no xlsx files')
            return [ dict(fname="", res=1) ]
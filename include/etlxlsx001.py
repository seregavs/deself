from  etlxlsx import XLSX_CSV_Base
import os
import pandas as pd
import logging
import platform


class XLSX_CSV_Data001(XLSX_CSV_Base):

    def read_excel(self, fname: str) -> int:
        try:
            col_names=['MATERIAL', 'PLANT', 'CALMONTH', 'AMOUNT','CURRENCY','COMMENT','SALEDATE']
            self.data = pd.read_excel(fname, sheet_name='продажи01', header = 0 \
                                , usecols='A:G', engine='openpyxl', dtype=str \
                                , names=col_names, skiprows=lambda x: x in [0,2])
            logging.info(f'EXTRACT,I,002,Reading {fname} was OK')
            return 0
        except Exception as e: 
            print(f'EXTRACT,E,002,Error in reading {fname}: {str(e)}') 
            logging.info(f'EXTRACT,E,002,Error in reading {fname}: {str(e)}')
            return 2

    def transform_data(self, fname: str) -> int:
        try:
            self.data['AMOUNT'] = self.data['AMOUNT'].astype('float')
            self.data['COMMENT'] = self.data['COMMENT'].replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"] \
                                                    , value=[" "," "], regex=True)
            self.data['SALEDATE'] = \
                  self.data['SALEDATE'].replace(to_replace=[r"(\d{1,2})/(\d{1,2})/(\d{4})"] \
                                        , value=["\\3"], regex=True) \
                + self.data['SALEDATE'].replace(to_replace=[r"(\d{1,2})/(\d{1,2})/(\d{4})"] \
                                        , value=["\\2"], regex=True).str.zfill(2) \
                + self.data['SALEDATE'].replace(to_replace=[r"(\d{1,2})/(\d{1,2})/(\d{4})"] \
                                        , value=["\\1"], regex=True).str.zfill(2) 
            # Если разделитель - часть строки то строка окаймляеся в кавычки!
            self.data['REQTSN'] = self.get_req_tsn()
            self.move_column_inplace(self.data,"REQTSN",0)
            logging.info(f'EXTRACT,I,003,Processing {fname} was OK')
            print(self.data)
            return 0
        except Exception as e: 
            print(f'EXTRACT,E,003,Error in processing {fname}: {str(e)}') 
            logging.info(f'EXTRACT,E,003,Error in processing {fname}: {str(e)}')
            return 3

    def save_csv(self, fname: str) -> int:
        try:
            # d  = self.data.to_csv('C:\\Users\\sshab\\Documents\\GitHub\\deself\\data\\dd.csv' \
            d  = self.data.to_csv(self.get_csv_filename(fname) \
                            ,  encoding='utf-8', index=False, header=True)
            logging.info(f'EXTRACT,I,004,Saving csv from {fname} was OK')   
            return 0
        except Exception as e: 
            print(f'EXTRACT,E,004,Error in saving csv from {fname}: {str(e)}') 
            logging.info(f'EXTRACT,E,004,Error in saving csv from {fname}: {str(e)}')      
            return 4
            # d  = data.to_csv('dd.csv',  encoding='utf-8', index=False)
            # data['DATUM'] = data['DATUM'].astype(str).apply(lambda x: x.replace('-', ''))
            # Assuming data types for `a` and `b` columns to be altered
            # pd.read_excel('file_name.xlsx', dtype={'a': np.float64, 'b': np.int32})


def helper_extract_file():
    if platform.system() == 'Windows':
        c_data_dir = os.environ.get('DAG_DATA_ROOT') \
            if os.environ.get('DAG_DATA_ROOT') != None \
                else 'C:\\Users\\sshab\\Documents\\GitHub\\deself\\data'
        c_fullpath = f'{c_data_dir}\\task01'
    else:
        c_data_dir = os.environ.get('DAG_DATA_ROOT') \
            if os.environ.get('DAG_DATA_ROOT') != None else 'deself_data'
        c_fullpath = f'/{c_data_dir}/task01'
    
    with XLSX_CSV_Data001(c_fullpath) as o:
        res = o.extract_file_from_dir()
    print(f'result = { res }')

if __name__ == "__main__":
    helper_extract_file()
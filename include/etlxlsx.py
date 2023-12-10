import os
import pandas as pd
import logging
import platform
from datetime import datetime
import time

def GetReqTSN() -> str:
    is_dst = time.daylight and time.localtime().tm_isdst > 0
    utc_offset = - (time.altzone if is_dst else time.timezone)/3600
    # I'm aware about timezones with fraction of hour offset. But it is not relevent for the case
    return datetime.now().strftime("%Y%m%d%H%M%S") + 'UTC'+str(int(utc_offset))

def move_column_inplace(df, col, pos):
    col = df.pop(col)
    df.insert(pos, col.name, col)

def SaveFile(fname, **op_kwargs ):
    try:
        col_names=['MATERIAL', 'PLANT', 'CALMONTH', 'AMOUNT','CURRENCY','COMMENT','SALEDATE']
        data = pd.read_excel(fname, sheet_name='продажи01', header = 0 \
                             , usecols='A:G', engine='openpyxl', dtype=str \
                             , names=col_names, skiprows=lambda x: x in [0,2])
        logging.info(f'EXTRACT,I,002,Reading {fname} was OK')
    except Exception as e: 
        print(f'EXTRACT,E,002,Error in reading {fname}: {str(e)}') 
        logging.info(f'EXTRACT,E,002,Error in reading {fname}: {str(e)}')
        return
    
    try:
        data['AMOUNT'] = data['AMOUNT'].astype('float')
        data['COMMENT'] = data['COMMENT'].replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=[" "," "], regex=True)
        data['SALEDATE'] = \
              data['SALEDATE'].replace(to_replace=[r"(\d{1,2})/(\d{1,2})/(\d{4})"], value=["\\3"], regex=True) \
            + data['SALEDATE'].replace(to_replace=[r"(\d{1,2})/(\d{1,2})/(\d{4})"], value=["\\2"], regex=True).str.zfill(2) \
            + data['SALEDATE'].replace(to_replace=[r"(\d{1,2})/(\d{1,2})/(\d{4})"], value=["\\1"], regex=True).str.zfill(2) 
        # Если разделитель - часть строки то строка окаймляеся в кавычки!
        data['REQTSN'] = GetReqTSN()
        move_column_inplace(data,"REQTSN",0)
        logging.info(f'EXTRACT,I,003,Processing {fname} was OK')
    except Exception as e: 
        print(f'EXTRACT,E,003,Error in processing {fname}: {str(e)}') 
        logging.info(f'EXTRACT,E,003,Error in processing {fname}: {str(e)}')
        return
    
    try:
        d  = data.to_csv('C:\\Users\\sshab\\Documents\\GitHub\\deself\\data\\dd.csv',  encoding='utf-8', index=False, header=True)
        logging.info(f'EXTRACT,I,004,Saving csv from {fname} was OK')   
    except Exception as e: 
        print(f'EXTRACT,E,004,Error in saving csv from {fname}: {str(e)}') 
        logging.info(f'EXTRACT,E,004,Error in saving csv from {fname}: {str(e)}')      
        return 
        # d  = data.to_csv('dd.csv',  encoding='utf-8', index=False)
        # data['DATUM'] = data['DATUM'].astype(str).apply(lambda x: x.replace('-', ''))
        # Assuming data types for `a` and `b` columns to be altered
        # pd.read_excel('file_name.xlsx', dtype={'a': np.float64, 'b': np.int32})
    
    print(data)
        # csv_file = data.to_csv(index=False, sep=';', header=False, line_terminator='\r\n' )
        # for i in range(len(data)):
        #     lst.append(str(data.iat[i,0]))
        #     lst.append(str(data.iat[i,1]))
        #     lst.append(str(data.iat[i,2]))
        # # print('lst=', lst)
        # for x in lst:
        #     for y in x.split(';'):
        #     lstd.append(str(y).strip())

        # lstd = list(set(lstd))
        # # print('lstd count is = ', len(lstd))
        # for x in lstd:
        #     print("CREATE USER {0} WITH IDENTITY '{0}' FOR SAML PROVIDER IDPDSA;".format(x))
        # c = input('enter to continue')
        # for x in lstd:
        #     print("INSERT INTO \"#ROLES\" (\"ROLE_NAME\",  \"PRINCIPAL_SCHEMA_NAME\", \"PRINCIPAL_NAME\") values ('wfuser', '', '{0}');".format(x) )

# 'C:\\Users\\sshab\\Documents\\GitHub\\deself\\data'
    # else 'data'  C:\Users\sshab\Documents\GitHub\deself\data \

def ExtractFileFromDir(c_fullpath: str):
    lst_files = [ fi for fi in os.listdir(c_fullpath) if fi[-4:] == 'xlsx']
    print(f'current working dir = { os.getcwd() }') # /lessons

    if len(lst_files) > 0:
        if platform.system() == 'Windows':
            fname = f'{c_fullpath}\\{lst_files[0]}' # Windows
        else:
            fname = f'{c_fullpath}/{lst_files[0]}' # docker
        SaveFile(fname)
    else:
        logging.info(f'EXTRACT,E,001,{c_fullpath} has no xlsx files')

def CopyToPostgre():
    pass

def HelperExtractFile():
    if platform.system() == 'Windows':
        c_data_dir = os.environ.get('DAG_DATA_ROOT') if os.environ.get('DAG_DATA_ROOT') != None else 'C:\\Users\\sshab\\Documents\\GitHub\\deself\\data'
        c_fullpath = f'{c_data_dir}\\task01'
    else:
        c_data_dir = os.environ.get('DAG_DATA_ROOT') if os.environ.get('DAG_DATA_ROOT') != None else 'deself_data'
        c_fullpath = f'/{c_data_dir}/task01'
    
    ExtractFileFromDir(c_fullpath)


if __name__ == "__main__":
    print(GetReqTSN())
    HelperExtractFile()
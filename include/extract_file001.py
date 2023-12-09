import os
import pandas as pd
import re

def SaveFile(fname):
    # print(f'filename = { fname}')
    lst = []
    lstd = []
    try:
        # MATERIAL	PLANT	CALMONTH	AMOUNT	CURRENCY COMMENT
        col_names=['MATERIAL', 'PLANT', 'CALMONTH', 'AMOUNT','CURRENCY','COMMENT','SALEDATE']
        data = pd.read_excel(fname, sheet_name='продажи01', header = 0 \
                             , usecols='A:G', engine='openpyxl', dtype=str \
                             , names=col_names, skiprows=lambda x: x in [0,2])
        data['AMOUNT'] = data['AMOUNT'].astype('float')
        data['COMMENT'] = data['COMMENT'].replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=[" "," "], regex=True)
        data['SALEDATE'] = \
              data['SALEDATE'].replace(to_replace=[r"(\d{1,2})/(\d{1,2})/(\d{4})"], value=["\\3"], regex=True) \
            + data['SALEDATE'].replace(to_replace=[r"(\d{1,2})/(\d{1,2})/(\d{4})"], value=["\\2"], regex=True).str.zfill(2) \
            + data['SALEDATE'].replace(to_replace=[r"(\d{1,2})/(\d{1,2})/(\d{4})"], value=["\\1"], regex=True).str.zfill(2) 
        # Если разделитель - часть строки то строка окаймляеся в кавычки!
        d  = data.to_csv('C:\\Users\\sshab\\Documents\\GitHub\\deself\\data\\dd.csv',  encoding='utf-8', index=False)
        # print(data.columns)
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

    except Exception as e: 
        print(str(e)) 

c_data_dir = os.environ.get('DAG_DATA_ROOT') if os.environ.get('DAG_DATA_ROOT') != None else 'C:\\Users\\sshab\\Documents\\GitHub\\deself\\data'
    # else 'data'  C:\Users\sshab\Documents\GitHub\deself\data \

c_fullpath = f'{c_data_dir}\\task01'

lst_files = []
lst_files = os.listdir(c_fullpath)

if len(lst_files) > 0:
    c_filepath = f'{c_fullpath}\\{lst_files[0]}'
    SaveFile(c_filepath)
    
import os

c_data_dir = os.environ.get('DAG_DATA_ROOT') if os.environ.get('DAG_DATA_ROOT') != None else 'data'
c_fullpath = f'{c_data_dir}\\task01'

lst_files = []
lst_files = os.listdir(c_fullpath)

if len(lst_files) > 0:
    c_filepath = f'{c_fullpath}\\"{lst_files[0]}"'
    print(c_filepath)
    
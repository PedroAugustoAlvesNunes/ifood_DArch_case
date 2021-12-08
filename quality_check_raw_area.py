import pandas as pd
from datetime import datetime as dt
import os
from utils.quality_definitions import define_schemas
from utils.logger import config_log

LOG = config_log()

def list_files():
    '''
    This function will list every file in today's folders inside our datalake
    This function will return a list of files
    '''
    # today = datetime.now()
    today = dt(2019, 1, 2)
    files = []
    base_path = f'datalake/raw/year={ today.strftime("%Y") }/month={ today.strftime("%m") }/day={ today.strftime("%d") }'
    for file in os.listdir(base_path):
        LOG.info(f'File { file } found')
        file_path = f'{ base_path }/{ file }'
        if os.path.isfile(file_path):
            files.append(file_path)
    return files

def check_file(file_path):
    '''
    This function will do a quality check on the file that it receives
    It will check for duplicates and schema validation
    args:
        file_path: path of file to quality check
    '''
    LOG.info(f'Reading data from { file_path }')
    df = pd.read_parquet(file_path).reset_index(drop=True)
    rows = len(df)
    df = df.drop_duplicates()
    LOG.info(f'Duplicates found { rows - len(df) }')

    #defining table based on file_path
    table = file_path.split('/')[-1].split('.')[0]
    errors = define_schemas()[table].validate(df)
    LOG.info(f'Errors found { len(errors) }')
    errors_index_rows = [e.row for e in errors]
    data_clean = df.drop(index=errors_index_rows)
    # save data
    if len(errors) > 0:
        LOG.info(f'Writing errors in errors_{ table }.csv')
        pd.DataFrame({'col':errors}).to_csv(f'errors_{ table }.csv')
    LOG.info(f'Writing clean dataframe into { file_path }')
    data_clean.to_parquet(file_path, compression='snappy', index=None)

def quality_check(file=None):
    if file:
        check_file(file)
    else:
        for file in list_files():
            check_file(file)

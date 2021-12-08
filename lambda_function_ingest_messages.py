from datetime import datetime as dt
import pandas as pd
from utils.logger import config_log

LOG = config_log()

def load_raw_data(message, date, table_name):
    '''
    This function will receive the message, date and table name (order or status).
    This function will update the file regarding the date and table received.
    args:
        message: message received with either order or status information.
        date: json with year, month and day of message
        table_name: table to be updated, either "status" or "order"
    '''
    LOG.info(f'Reading already existing data')
    output_file = f"datalake/raw/year={date['year']}/month={date['month']}/day={date['day']}/{table_name}.parquet"
    original_df = pd.read_parquet(output_file)
    message_df = pd.DataFrame([message])
    LOG.info(f'Updating data')
    original_df = original_df.append(message_df, ignore_index=True)
    LOG.info(f'Writing updated dataframe to { output_file }')
    original_df.to_parquet(output_file, compression='snappy', index=None)

def get_date(date):
    '''
    This function will receive a date in string format
    This function will return an json with year, month and day keys based on date received.
    args:
        date: date in string format ('%Y-%m-%dT%H:%M:%S.%fZ')
    '''
    date = dt.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
    return {
        'year': date.year,
        'month': date.strftime('%m'),
        'day': date.strftime('%d')
    }

def ingest_order_message(message):
    '''
    This function will receive a message regarding a order.
    The message will be processed and inserted (load_raw_data) into the proper file, based on the order_created_at field
    args:
        message: message with a valid json
    '''
    LOG.info(f'Ingesting order')
    load_raw_data(message, get_date(message['order_created_at']), 'order')

def ingest_status_message(message):
    '''
    This function will receive a message regarding the status of a order.
    The message will be processed and inserted (load_raw_data) into the proper file, based on the created_at field
    args:
        message: message with a valid json
    '''
    LOG.info(f'Ingesting status')
    load_raw_data(message, get_date(message['created_at']), 'status')

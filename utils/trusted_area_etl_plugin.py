import os
import pandas as pd
from datetime import datetime as dt, timedelta as td
import numpy as np
import hashlib
import json
from utils.logger import config_log

LOG = config_log()

def get_order(date=dt.now()):
    LOG.info('Reading orders data')
    date=dt(2019, 1, 2)
    order = pd.read_parquet(f'datalake/raw/year={ date.strftime("%Y") }/month={ date.strftime("%m") }/day={ date.strftime("%d") }/order.parquet')
    order = order.drop(columns='customer_name')
    return order

def get_status(date=dt.now()):
    LOG.info('Reading status data')
    date=dt(2019, 1, 2)
    status = pd.read_parquet(f'datalake/raw/year={ date.strftime("%Y") }/month={ date.strftime("%m") }/day={ date.strftime("%d") }/status.parquet')
    status = status.rename(columns={'created_at':'status_created_at', 'value':'status_value'})
    return status

def get_last_status(status):
    LOG.info('Getting orders last status')
    last_status = status.groupby(['order_id'], as_index=False).agg({'status_created_at':'max'}).merge(status, how='left', on=['order_id', 'status_created_at'])
    last_status = last_status.drop_duplicates('order_id')
    return last_status

def get_restaurant():
    LOG.info('Reading restaurants data')
    restaurant = pd.read_parquet('datalake/raw/restaurant.parquet')
    restaurant = restaurant.rename(
        columns={
            'id':'merchant_id',
            'created_at':'merchant_created_at',
            'enabled':'merchant_enabled',
            'price_range':'merchant_price_range',
            'average_ticket':'merchant_average_ticket',
            'takeout_time':'merchant_takeout_time',
            'delivery_time':'merchant_delivery_time',
            'minimum_order_value':'merchant_minimum_order_value',
            }
        )
    return restaurant

def get_consumer():
    LOG.info('Reading costumers data')
    consumer = pd.read_parquet('datalake/raw/consumer.parquet')
    consumer = consumer.rename(
        columns={
            'language':'customer_language',
            'created_at':'customer_created_at',
            'active':'customer_active'
        }
    )
    return consumer

def update_local_date(order, states, delta):
    LOG.info(f"Updating date based on states { ','.join(states) } timezone")
    order['order_local_created_at'] = np.where(
        order['merchant_state'].isin(states),
            order['order_created_at'].apply(lambda date: (dt.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ') - delta).date()), order['order_created_at'].apply(lambda date: dt.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ').date()))
    return order

def anonymize_columns(df, columns):
    LOG.info(f"Anonymizing columns { ','.join(columns) }")
    for column in columns:
        df[column] = df[column].apply(
            lambda value: hashlib.sha256(str(value).encode()).hexdigest()
        )
    return df

def update_dataset(new_dataset, file_path):
    LOG.info(f"Updating dataframe on filepath { file_path }")
    original_dataset = pd.read_parquet(file_path)
    original_dataset = original_dataset.append(new_dataset, ignore_index=True)
    original_dataset.to_parquet(file_path, compression='snappy', index=None)  

def write_dataset_with_partition(dataset, partition, table):
    for day in dataset[partition].unique():
        file_path = f"datalake/trusted/year={ day.strftime('%Y') }/month={ day.strftime('%m') }/day={ day.strftime('%d') }/{ table }.parquet"
        partition_dataset = dataset.loc[dataset[partition] == day]
        if os.path.isfile(file_path):
            update_dataset(partition_dataset, file_path)
        else:
            LOG.info(f"Creating dataframe on filepath { file_path }")
            partition_dataset.to_parquet(file_path, compression='snappy', index=None)


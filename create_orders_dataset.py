from utils.trusted_area_etl_plugin import *
from utils.logger import config_log

LOG = config_log()

def orders_dataset():
    '''
    This function will read order, status, restaurant and consumer dataframs from RAW area.
    This function will do transformations, and write the final result to Trusted area.
    '''
    order = get_order()
    status = get_status()
    restaurant = get_restaurant()
    consumer = get_consumer()
    last_status = get_last_status(status)
    LOG.info(f"Merging dataframes")
    order = order.merge(last_status, how='left', on='order_id')
    order = order.merge(restaurant,  how='left', on='merchant_id')
    order = order.merge(consumer,  how='left', on='customer_id')
    order = update_local_date(order, ['AM','RR','RO','MT','MS'], td(hours=1))
    order = update_local_date(order, ['AC'], td(hours=2))
    anonymize_columns(order, ['customer_name', 'customer_phone_number', 'cpf', 'delivery_address_latitude', 'delivery_address_longitude', 'delivery_address_zip_code'])
    write_dataset_with_partition(order, 'order_local_created_at', 'orders')

if __name__=='__main__':
    LOG.info(f"Starting to create orders_dataset")
    orders_dataset()

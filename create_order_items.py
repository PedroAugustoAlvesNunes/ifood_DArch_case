from utils.trusted_area_etl_plugin import *
import multiprocessing as mp
from utils.logger import config_log

LOG = config_log()

def get_items(order_id, items):
    '''
    This functions will receive a list of jsons and return a dataframe with the order_id attached to it
    args:
        order_id: order_id to be attached to the dataframe
        items: list of jsons containing items information
    '''
    dataset_items = pd.json_normalize(json.loads(items), sep='_')
    dataset_items['order_id'] = order_id
    return dataset_items

def order_items_dataset(dataset):
    '''
    This function will receive a dataset and return a list of datasets. One dataset for each row
    args:
        dataset: dataset of orders
    '''
    series_of_datasets = dataset.apply(lambda row: get_items(row['order_id'], row['items']), axis=1)
    return pd.concat(series_of_datasets.values.tolist())

if __name__=='__main__':
    '''
    This script will read the orders dataset and use multiprocessing to create the items dataset
    '''
    LOG.info(f"Starting to create items dataset")
    order = get_order()
    order = order.reset_index(drop=True)
    rows = len(order)
    #pool = mp.Pool(mp.cpu_count - 1)
    pool = mp.Pool(5)
    slice_end = 0
    dataframes = []
    #day = dt.now()
    day = dt(2019, 1, 2)
    LOG.info(f'Slicing dataframe to multiprocess it')
    while slice_end < rows:
        slice_start = slice_end
        slice_end += 1000
        dataframes.append(order.iloc[slice_start:slice_end])
    items = pd.concat(pool.map(order_items_dataset, dataframes))
    file_path = f"datalake/trusted/year={ day.strftime('%Y') }/month={ day.strftime('%m') }/day={ day.strftime('%d') }/order_items.parquet"
    if os.path.isfile(file_path):
        update_dataset(items, file_path)
    else:
        LOG.info(f"Creating dataframe on filepath { file_path }")
        items.to_parquet(file_path, compression='snappy', index=None)

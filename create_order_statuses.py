from utils.trusted_area_etl_plugin import *

if __name__=='__main__':
    '''
    This script will read the status table and create a new table with one row per order and each status with it's own column.
    '''
    status = get_status()
    status.pivot_table('status_created_at', 'order_id', 'status_value', {'status_created_at':'max'})
    day = dt(2019, 1, 2)
    file_path = f"datalake/trusted/year={ day.strftime('%Y') }/month={ day.strftime('%m') }/day={ day.strftime('%d') }/order_statuses.parquet"
    if os.path.isfile(file_path):
        update_dataset(status, file_path)
    else:
        LOG.info(f"Creating dataframe on filepath { file_path }")
        status.to_parquet(file_path, compression='snappy', index=None)

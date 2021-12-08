import json
import pandas_schema
from pandas_schema import Column
from pandas_schema.validation import CustomElementValidation
import numpy as np
from decimal import *
from utils.logger import config_log

LOG = config_log()

def check_decimal(dec):
    '''
    This function will check if parameter is decimal
    args:
        dec: parameter to be checked
    '''
    try:
        Decimal(dec)
    except InvalidOperation:
        return False
    return True

def check_int(num):
    '''
    This function will check if parameter is int
    args:
        num: parameter to be checked
    '''
    try:
        int(num)
    except ValueError:
        return False
    return True

def check_items_list(items):
    '''
    This function will check if parameter is a list
    args:
        items: parameter to be checked
    '''
    try:
        return True if isinstance(json.loads(items), list) else False
    except json.JSONDecodeError:
        return False

def define_schemas():
    '''
    This function will create and return the schemas for validation used on quality_check.py
    '''
    decimal_validation = [CustomElementValidation(lambda d: check_decimal(d), 'is not decimal')]
    int_validation = [CustomElementValidation(lambda i: check_int(i), 'is not integer')]
    items_validation = [CustomElementValidation(lambda d: check_items_list(d), 'is not a list of items')]
    null_validation = [CustomElementValidation(lambda d: d is not np.nan, 'this field cannot be null')]
    return {
        'order': pandas_schema.Schema([
            Column('cpf', int_validation + null_validation),
            Column('customer_id', null_validation),
            Column('customer_name', null_validation),
            Column('delivery_address_city', null_validation),
            Column('delivery_address_country', null_validation),
            Column('delivery_address_district', null_validation),
            Column('delivery_address_external_id', null_validation),
            Column('delivery_address_latitude'),
            Column('delivery_address_longitude'),
            Column('delivery_address_state', null_validation),
            Column('delivery_address_zip_code',int_validation +  null_validation),
            Column('items', items_validation),
            Column('merchant_id', null_validation),
            Column('merchant_latitude'),
            Column('merchant_longitude'),
            Column('merchant_timezone'),
            Column('order_created_at', null_validation),
            Column('order_id', null_validation),
            Column('order_scheduled'),
            Column('order_total_amount', decimal_validation + null_validation),
            Column('origin_platform'),
            Column('order_scheduled_date')
        ]),
        'status': pandas_schema.Schema([
            Column('order_id', null_validation),
            Column('status_id', null_validation),
            Column('value', null_validation),
            Column('created_at', null_validation)
        ])
    }

import json
import logging

def extract():
    data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
    return json.loads(data_string)     

def transform(order_data):
    logging.info(type(order_data))
    total_order_value = 0.0
    for value in order_data.values():
        total_order_value += value
    logging.info(f'total_order_value is {total_order_value}')
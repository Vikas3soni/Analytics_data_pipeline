import pandas as pd
from datetime import datetime

def calculate_days_(start_date, end_date):
    diff = (end_date - start_date).days
    if diff < 0:
        return 0
    else:
        return diff


def calculate_days(start_date, end_date):
    start_date = datetime.strptime(start_date, '%Y/%m/%d, %H:%M:%S')
    end_date = datetime.strptime(end_date, '%Y/%m/%d, %H:%M:%S')
    diff = (end_date - start_date).days
    if diff < 0:
        return 0
    else:
        return diff


def get_date_id_from_datetime(datetime):
    return datetime.strftime('%Y%m%d')

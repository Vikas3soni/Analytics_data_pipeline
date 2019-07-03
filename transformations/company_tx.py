import bson
from .utils import get_date_id_from_datetime
from datetime import datetime


def create(payload):
    company_id = bson.string_type(payload.get('_id'))
    company_name = payload.get('name')
    company_lic_no = payload.get('liscenseNo')
    company_created_on_date_id = get_date_id_from_datetime(payload.get('createdAt'))
    company_created_on_date = payload.get('createdAt').strftime("%Y/%m/%d, %H:%M:%S")
    company_status = payload.get('status')
    domain = payload.get('domain')
    is_active_record = 1
    valid_from_date = datetime.now().strftime("%Y/%m/%d, %H:%M:%S")
    valid_till_date = datetime.max.strftime("%Y/%m/%d, %H:%M:%S")
    valid_from_date_id = get_date_id_from_datetime(datetime.now())
    valid_till_date_id = get_date_id_from_datetime(datetime.max)

    row = {'company_id': company_id, 'company_name': company_name,
           'company_created_on_date_id': company_created_on_date_id,
           'company_lic_no': company_lic_no, 'company_status': company_status,
           'company_created_on_date': company_created_on_date,
           'domain': domain, 'is_active_record': is_active_record,
           'valid_from_date': valid_from_date,
           'valid_till_date': valid_till_date,
           'valid_from_date_id': valid_from_date_id,
           'valid_till_date_id': valid_till_date_id
           }
    return row

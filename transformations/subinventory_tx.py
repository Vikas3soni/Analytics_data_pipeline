import bson
from datetime import datetime
from .utils import get_date_id_from_datetime


def create(payload):
    sub_inventory_id = bson.string_type(payload.get('_id'))
    company_id = bson.string_type(payload.get('companyId'))
    sub_inventory_name = payload.get('code') or "Not Available"
    sub_inventory_status = payload.get('status') or "Not Available"
    org_id = bson.string_type(payload.get('organization'))
    sub_inventory_display_name = ""
    is_active_record = 1
    valid_from_date = datetime.now().strftime("%Y/%m/%d, %H:%M:%S")
    valid_till_date = datetime.max.strftime("%Y/%m/%d, %H:%M:%S")
    valid_from_date_id = get_date_id_from_datetime(datetime.now())
    valid_till_date_id = get_date_id_from_datetime(datetime.max)

    row = {'sub_inventory_id': sub_inventory_id, 'company_id': company_id,
           'org_id': org_id,
           'sub_inventory_status': sub_inventory_status,
           'sub_inventory_name': sub_inventory_name,
           'sub_inventory_display_name': sub_inventory_display_name,
           'is_active_record': is_active_record,
           'valid_from_date': valid_from_date,
           'valid_till_date': valid_till_date,
           'valid_from_date_id': valid_from_date_id,
           'valid_till_date_id': valid_till_date_id
           }

    return row

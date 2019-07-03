import bson
from datetime import datetime
from .utils import get_date_id_from_datetime

def create(payload):
    plant_batch_id = bson.string_type(payload.get('_id'))
    plant_batch_created_on_date_id = get_date_id_from_datetime(payload.get('createdOn'))
    plant_batch_created_on_date= payload.get('createdOn').strftime("%Y/%m/%d, %H:%M:%S")
    plant_batch_name = payload.get('name')
    plant_batch_display_id = payload.get('plantBatchId')
    strain_id = payload.get('strain')
    org_id = bson.string_type(payload.get('organization'))
    company_id = bson.string_type(payload.get('companyId'))
    plantation_method = payload.get('plantationType')
    is_active_record = 1
    valid_from_date = datetime.now().strftime("%Y/%m/%d, %H:%M:%S")
    valid_till_date = datetime.max.strftime("%Y/%m/%d, %H:%M:%S")
    valid_from_date_id = get_date_id_from_datetime(datetime.now())
    valid_till_date_id = get_date_id_from_datetime(datetime.max)

    row = {'plant_batch_id': plant_batch_id,
           'plant_batch_created_on_date': plant_batch_created_on_date,
           'plant_batch_created_on_date_id': plant_batch_created_on_date_id,
           'plant_batch_name': plant_batch_name,
           'plant_batch_display_id': plant_batch_display_id,
           'strain_id': strain_id, 'org_id': org_id,
           'plantation_method': plantation_method,
           'company_id': company_id, 'is_active_record': is_active_record,
           'valid_from_date': valid_from_date,
           'valid_till_date': valid_till_date,
           'valid_from_date_id': valid_from_date_id,
           'valid_till_date_id': valid_till_date_id
           }
    return row

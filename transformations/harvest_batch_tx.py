import bson
from datetime import datetime
from .utils import get_date_id_from_datetime


def create(payload):
    harvest_batch_id = bson.string_type(payload.get('_id'))
    strain_id = bson.string_type(payload.get('strain').get('value'))
    batch_created_on_date = payload.get('createdOn').strftime("%Y/%m/%d, %H:%M:%S")
    batch_created_on_date_id = get_date_id_from_datetime(payload.get('createdOn'))
    sub_inventory_id = bson.string_type(payload.get('subInventory').get('id'))
    plant_batch_id = bson.string_type(payload.get('plantBatch').get('id'))
    total_weight = payload.get('initialWeight').get('weight')
    batch_weight_available = total_weight - payload.get('usedWeight').get('weight')
    total_waste = sum([waste.get('weight') for waste in payload.get('waste')])
    company_id = bson.string_type(payload.get('companyId'))
    org_id = bson.string_type(payload.get('organization').get('id'))
    is_active_record = 1
    valid_from_date = datetime.now().strftime("%Y/%m/%d, %H:%M:%S")
    valid_till_date = datetime.max.strftime("%Y/%m/%d, %H:%M:%S")
    valid_from_date_id = get_date_id_from_datetime(datetime.now())
    valid_till_date_id = get_date_id_from_datetime(datetime.max)

    row = {'harvest_batch_id': harvest_batch_id, 'strain_id': strain_id,
           'batch_created_on_date_id': batch_created_on_date_id,
           'batch_created_on_date': batch_created_on_date,
           'sub_inventory_id': sub_inventory_id,
           'plant_batch_id': plant_batch_id,
           'total_weight': total_weight,
           'batch_weight_available': batch_weight_available,
           'total_waste': total_waste,
           'company_id': company_id,
           'org_id': org_id,
           'is_active_record': is_active_record,
           'valid_from_date': valid_from_date,
           'valid_till_date': valid_till_date,
           'valid_from_date_id': valid_from_date_id,
           'valid_till_date_id': valid_till_date_id,
           'plant_batch_id':plant_batch_id
           }

    return row

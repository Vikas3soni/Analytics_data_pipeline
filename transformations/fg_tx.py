import bson
from datetime import datetime
from .utils import get_date_id_from_datetime
from caching import item_sku


def create(payload, fg_type, source_type,conn):
    fg_id = bson.string_type(payload.get('_id'))
    fg_type = fg_type
    source_type = source_type
    company_id = bson.string_type(payload.get('companyId'))
    org_id = bson.string_type(payload.get('organization').get('id'))
    strain_id = "Not Available" # bson.string_type(payload.get('strain').get('id')) or
    item_id = bson.string_type(payload.get('item').get('id'))
    package_tag_id = bson.string_type(payload.get('packageTag').get('id'))
    sub_inventory_id = bson.string_type(payload.get('subInventory').get('id'))
    fg_status = 'completely_in_inventory'
    item_sku_unit_quantity = payload.get('unitCount')
    item_sku_unit_count = payload.get('unitQuantity')
    total_fg_weight = payload.get('totalLotWeight').get('weight') if fg_type == 'lot' else payload.get('initialWeight').get('weight')
    fg_created_date = payload.get('lotManufacturingDate')
    fg_expiry_date = payload.get('lotExpirationDate')
    fg_created_date_id = get_date_id_from_datetime(fg_created_date)
    fg_expiry_date_id = get_date_id_from_datetime(fg_expiry_date)
    is_active_record = 1
    valid_from_date = datetime.now()
    valid_till_date = datetime.max
    valid_from_date_id = get_date_id_from_datetime(valid_from_date)
    valid_till_date_id = get_date_id_from_datetime(valid_till_date)

    available_fg_weight = total_fg_weight

    item_sku_id = item_sku.find_sku_id(bson.string_type(payload.get('item').get('id')),
                                       payload.get("unitQuantity"), conn)

    row = {'fg_id': fg_id, 'fg_type': fg_type, 'source_type': source_type,
           'company_id': company_id, 'org_id': org_id,
           'strain_id': strain_id,  'item_id': item_id,
           'package_tag_id': package_tag_id,
           'sub_inventory_id': sub_inventory_id, 'fg_status': fg_status,
           'item_unit_unit_quantity': item_sku_unit_quantity,
           'total_fg_weight': total_fg_weight,
           'item_sku_unit_count': item_sku_unit_count,
           'fg_created_date_id': fg_created_date_id,
           'fg_expiry_date_id': fg_expiry_date_id,
           'is_active_record': is_active_record,
           'valid_from_date': valid_from_date,
           'valid_till_date': valid_till_date,
           'valid_from_date_id': valid_from_date_id,
           'valid_till_date_id': valid_till_date_id,
           'available_fg_weight': available_fg_weight,
           'item_sku_id': item_sku_id
           }

    return row


# for relationship table
def fg_source_table_update(payload, fg_type, source_type):
    fg_id = bson.string_type(payload.get('_id'))
    fg_type = fg_type
    source_type = source_type
    list_to_send = []
    for item in payload.get('materialDetails'):
        source_str = "plantMaterial" if fg_type == 'lot' else "harvestMaterial"
        source_id = item.get(source_str).get('id')
        amount_used = item.get(source_str).get('lotweight').get('weight')
        row = {'fg_id': fg_id, 'fg_type': fg_type,
               'source_type': source_type, 'source_id': source_id,
               'amount_used': amount_used
               }
        list_to_send.append(row)

    return list_to_send


# for item inventory table
def item_inventory_table_update(payload):
    item_sku_id = item_sku.find_sku_id(bson.string_type(payload.get('item').get('id')),
                                       payload.get("unitQuantity"))

    row = {'item_sku_id': item_sku_id,
           'available_sku_count': payload.get('unitQuantity'),
           'available_sku_quantity': payload.get('unitCount')
           }

    return row

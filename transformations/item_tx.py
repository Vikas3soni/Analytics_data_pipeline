import bson
from datetime import datetime
from .utils import get_date_id_from_datetime


def create(payload):
    rows = []
    item_id = bson.string_type(payload.get('_id'))
    item_sku_currency = payload.get('currency').get('code')
    item_status = payload.get('status')
    item_name = payload.get('itemName')
    item_type = payload.get('itemType')
    item_no = payload.get('itemNo')
    item_category = payload.get('itemCategory')
    item_sub_status = payload.get('itemSubStatus')
    company_id = bson.string_type(payload.get('companyId'))
    org_id = bson.string_type(payload.get('organizations')[0])
    item_reserved_qty = payload.get('reservedQuantity')
    item_sku_uom = payload.get('primaryUomCode')
    minimum_quantity_to_buy = payload.get('minimumQuantityToBuy')
    is_active_record = 1
    valid_from_date = datetime.now().strftime("%Y/%m/%d, %H:%M:%S")
    valid_till_date = datetime.max.strftime("%Y/%m/%d, %H:%M:%S")
    valid_from_date_id = get_date_id_from_datetime(datetime.now())
    valid_till_date_id = get_date_id_from_datetime(datetime.max)
    for item_sku in payload.get('priceDetails'):
        item_sku_id = bson.string_type(item_sku.get('_id'))
        item_sku_price = item_sku.get('price')
        item_sku_quantity_per_unit = item_sku.get('unitCount')
        item_sku_status = item_sku.get('status')
        item_sku_name = str(item_name) + "_" + str(item_sku_price)

        row = {'item_sku_id': item_sku_id, 'item_id': item_id,
               'item_sku_currency': item_sku_currency, 'item_status': item_status,
               'item_name': item_name, 'item_category': item_category,
               'company_id': company_id, 'org_id': org_id,
               'item_reserved_qty': item_reserved_qty,
               'minimum_quantity_to_buy': minimum_quantity_to_buy,
               'item_sku_price': item_sku_price,
               'item_sku_quantity_per_unit': item_sku_quantity_per_unit,
               'item_sku_uom': item_sku_uom,
               'item_sku_status': item_sku_status,
               'item_sku_name': item_sku_name,
               'item_type': item_type,
               'item_no': item_no,
               'item_sub_status': item_sub_status,
               'is_active_record': is_active_record,
               'valid_from_date': valid_from_date,
               'valid_till_date': valid_till_date,
               'valid_from_date_id': valid_from_date_id,
               'valid_till_date_id': valid_till_date_id
               }
        rows.append(row)
    return rows

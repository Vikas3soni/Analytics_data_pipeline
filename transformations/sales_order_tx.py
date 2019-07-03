import bson
import os
import sys
from datetime import datetime
from .utils import get_date_id_from_datetime
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from caching import item_sku


def create(payload):
    """FACT TABLE"""
    order_id = bson.stringtype(payload.get('_id'))
    order_system_id = bson.string_type(payload.get('id'))
    customer_id = bson.string_type(payload.get('customer').get('id'))
    supplier_id = bson.string_type(payload.get('supplier').get('id'))
    order_status = bson.string_type(payload.get('status'))
    order_date = payload.get('orderdate').get('seconds')
    order_date_id = get_date_id_from_datetime(order_date)
    payment_method = payload.get('payment').get('paymentmethod')
    org_id = bson.string_type(payload.get('orgid')[0])
    company_id = payload.get('companyid')
    total_amount_paid = sum([item.get('price').get('price') for item in payload.get('saleproducts')])
    total_amount_currency = ""
    for item in payload.get('saleproducts'):
        total_amount_currency = item.get('price').get('price')
        break
    discount = 0
    last_order_status_change_date = order_date
    last_order_status_change_date_id = get_date_id_from_datetime(last_order_status_change_date)
    is_active_record = 1
    valid_from_date = datetime.now()
    valid_till_date = datetime.max
    valid_from_date_id = get_date_id_from_datetime(valid_from_date)
    valid_till_date_id = get_date_id_from_datetime(valid_till_date)

    row = {"order_id": order_id, "order_system_id": order_system_id,
           "customer_id": customer_id, "supplier_id":  supplier_id,
           "order_status": order_status, "order_date": order_date,
           "order_date_id": order_date_id, "payment_method": payment_method,
           "org_id": org_id, "company_id": company_id,
           "total_amount_paid": total_amount_paid,
           "total_amount_currency": total_amount_currency,
           "discount": discount, "is_active_record": is_active_record,
           "valid_from_date": valid_from_date,
           "valid_till_date": valid_till_date,
           "valid_from_date_id": valid_from_date_id,
           "valid_till_date_id": valid_till_date_id
           }
    return row


def item_order_map(payload):
    """item - order item mapping relationship table create/update from sales event"""
    order_id = bson.string_type(payload.get("_id"))
    rows = []
    for item_order in payload.get("saleproducts"):

        item_id = bson.string_type(item_order.get('itemid'))
        item_sku_unit_quantity = item_order.get('weight').get('weight')
        item_sku_id = item_sku.find_sku_id(item_id, item_sku_unit_quantity)

        item_sku_pay = item_order.get('price').get('price')
        item_sku_currency = item_order.get('price').get('currencycode')
        discount = 0
        amount_paid = item_sku_pay - discount
        item_sku_quantity_ordered = item_order.get('quantity')

        row = {
            "order_id": order_id,
            "item_sku_id": item_sku_id,
            "item_id": item_id,
            "amount_paid": amount_paid,
            "item_sku_currency": item_sku_currency,
            "item_sku_quantity_ordered": item_sku_quantity_ordered
        }

        rows.append(row)
    return rows


def sales_order_fg_map(payload):
    """relation ship table sales order fg mapping create/update from sales order event"""
    order_id = bson.string_type(payload.get("_id"))
    rows = []
    for packages in payload.get("packages"):
        for products in packages.get("products"):
            for fullfilment in products.get("fullfillment"):
                source_id = fullfilment.get("sourceid")
                row = {
                    "order_id": order_id,
                    "fg_id": source_id
                    }
                rows.append(row)
    return rows


def item_inventory_update(payload):
    """item inventory fact table update from sales order event"""
    is_active_record = 1
    valid_from_date = datetime.now()
    valid_till_date = datetime.max
    valid_from_date_id = get_date_id_from_datetime(valid_from_date)
    valid_till_date_id = get_date_id_from_datetime(valid_till_date)
    rows = []
    for packages in payload.get("packages"):
        for products in packages.get("products"):
            # TODO: SKU_ID
            item_sku_id = item_sku.find_sku_id(bson.string_type(products.get("itemid")),
                                               products.get("weight").get("weight"))
            available_sku_count = products.get("quantity")
            available_sku_quantity = products.get("weight").get("weight")
            row = {
                "item_sku_id": item_sku_id,
                "available_sku_count": available_sku_count,
                "available_sku_quantity": available_sku_quantity,
                "is_active_record": is_active_record,
                "valid_from_date": valid_from_date,
                "valid_till_date": valid_till_date,
                "valid_from_date_id": valid_from_date_id,
                "valid_till_date_id": valid_till_date_id
                }
            rows.append(row)
    return rows


def fg_update(payload):
    """fg fact table update from sale order event"""
    is_active_record = 1
    valid_from_date = datetime.now()
    valid_till_date = datetime.max
    valid_from_date_id = get_date_id_from_datetime(valid_from_date)
    valid_till_date_id = get_date_id_from_datetime(valid_till_date)

    rows = []
    for packages in payload.get("packages"):
        for products in packages.get("products"):
            each_sku_quantity = products.get("weight").get("weight")
            for fullfilment in products.get("fullfillment"):
                source_id = fullfilment.get("sourceid")
                row = {
                    "used_fg_weight": fullfilment.get("quantity") * each_sku_quantity,
                    "fg_id": source_id,
                    "is_active_record": is_active_record,
                    "valid_from_date": valid_from_date,
                    "valid_till_date": valid_till_date,
                    "valid_from_date_id": valid_from_date_id,
                    "valid_till_date_id": valid_till_date_id
                    }
                rows.append(row)
    return rows

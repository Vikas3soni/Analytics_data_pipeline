import bson
from datetime import datetime
from .utils import get_date_id_from_datetime


def create(payload):
    customer_id = bson.string_type(payload.get("id"))
    customer_firstname = payload.get("basicInfo").get("firstName")
    customer_lastname = payload.get("basicInfo").get("lastName")
    customer_middlename = payload.get("basicInfo").get("middleName")
    customer_email = payload.get("basicInfo").get("email")
    customer_status = payload.get("status")
    customer_approval_status = payload.get("businessCustomerStatus")
    customer_profile_type = payload.get("profileType")
    customer_role_type = payload.get("roleType").get('value')
    billing_address = {}
    shipping_address = {}
    for address in payload.get("addressBook"):
        if address.get("addressType") == "shipping":
            shipping_address = address
            continue
        if address.get("addressType") == "billing":
            billing_address = address
    customer_billing_zip = billing_address.get("zipCode")
    customer_billing_city = billing_address.get("city")
    customer_billing_state = billing_address.get("state")
    customer_billing_counrty = billing_address.get("country")
    customer_shipping_zip = shipping_address.get("zipCode")
    customer_shipping_city = shipping_address.get("city")
    customer_shipping_state = shipping_address.get("state")
    customer_shipping_counrty = shipping_address.get("country")
    is_active_record = 1
    valid_from_date = datetime.now()
    valid_till_date = datetime.max
    valid_from_date_id = get_date_id_from_datetime(valid_from_date)
    valid_till_date_id = get_date_id_from_datetime(valid_till_date)

    row = {"customer_id": customer_id,
           "customer_firstname": customer_firstname,
           "customer_lastname": customer_lastname,
           "customer_middlename": customer_middlename,
           "customer_email": customer_email,
           "customer_status": customer_status,
           "customer_approval_status": customer_approval_status,
           "customer_profile_type": customer_profile_type,
           "customer_role_type": customer_role_type,
           "customer_billing_zip": customer_billing_zip,
           "customer_billing_city": customer_billing_city,
           "customer_billing_state": customer_billing_state,
           "customer_billing_counrty": customer_billing_counrty,
           "customer_shipping_zip": customer_shipping_zip,
           "customer_shipping_city": customer_shipping_city,
           "customer_shipping_state": customer_shipping_state,
           "customer_shipping_counrty": customer_shipping_counrty,
           'is_active_record': is_active_record,
           'valid_from_date': valid_from_date,
           'valid_till_date': valid_till_date,
           'valid_from_date_id': valid_from_date_id,
           'valid_till_date_id': valid_till_date_id
           }
    return row

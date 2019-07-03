import bson
from datetime import datetime
from .utils import get_date_id_from_datetime


def create(payload):
    org_id = bson.string_type(payload.get('_id'))
    org_name = payload.get('name')
    org_type = payload.get('type').get('value') or "Not Available"
    org_zip = payload.get('address')[0].get('zipcode') or "Not Available"
    org_statecode = payload.get('address')[0].get('stateCode') or "Not Available"
    org_countrycode = payload.get('address')[0].get('countryCode') or "Not Available"
    org_city = payload.get('address')[0].get('city') or "Not Available"
    org_status = payload.get('status') or "Not Available"
    company_id = bson.string_type(payload.get('companyId'))
    org_lic_no = payload.get('license') or "Not Available"
    org_created_on_date = payload.get('createdOn').strftime("%Y/%m/%d, %H:%M:%S")
    org_created_on_date_id = get_date_id_from_datetime(payload.get('createdOn'))
    is_active_record = 1
    valid_from_date = datetime.now().strftime("%Y/%m/%d, %H:%M:%S")
    valid_till_date = datetime.max.strftime("%Y/%m/%d, %H:%M:%S")
    valid_from_date_id = get_date_id_from_datetime(datetime.now())
    valid_till_date_id = get_date_id_from_datetime(datetime.max)

    row = {'org_id': org_id, 'org_name': org_name, 'org_type': org_type,
           'org_zip': org_zip, 'org_city': org_city,
           'org_countrycode': org_countrycode, 'org_statecode': org_statecode,
           'org_status': org_status, 'org_lic_no': org_lic_no,
           'company_id': company_id,
           'org_created_on_date': org_created_on_date,
           'org_created_on_date_id': org_created_on_date_id,
           'is_active_record': is_active_record,
           'valid_from_date': valid_from_date,
           'valid_till_date': valid_till_date,
           'valid_from_date_id': valid_from_date_id,
           'valid_till_date_id': valid_till_date_id
           }

    return row

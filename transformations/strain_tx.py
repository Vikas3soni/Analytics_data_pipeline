import bson
from datetime import datetime
from .utils import get_date_id_from_datetime

def create(payload):
    strain_id = bson.string_type(payload.get('_id')) or bson.string_type(payload.get('strainId'))
    strain_status = payload.get('status') or "Not Available"
    strain_name = payload.get('name') or "Not Available"
    indica_level = payload.get('indicaLevel') or 0
    sativa_level = payload.get('sativaLevel') or 0
    strain_created_on_date = payload.get('createdOn').strftime("%Y/%m/%d, %H:%M:%S")
    strain_created_on_date_id = get_date_id_from_datetime(payload.get('createdOn'))
    tetra_hydro_cannabinol = payload.get("tetraHydroCannabinol") or 0
    canna_bichromene = payload.get("cannaBiChromene") or 0
    tetra_hydro_cannabinolic = payload.get("tetraHydroCannabinolic") or 0
    canna_bicyclol = payload.get("cannaBiCyclol") or 0
    canna_bivarin = payload.get("cannaBiVarin") or 0
    canna_bodiolic_acide = payload.get("cannaBoDiolicAcide") or 0
    tetra_hydro_cannabi_varin = payload.get("tetraHydroCannabiVarin") or 0
    canna_bi_diol = payload.get("cannaBiDiol") or 0
    canna_bi_nol = payload.get("cannaBiNol") or 0
    canna_bi_gerol = payload.get("cannaBiGerol") or 0
    company_id = bson.string_type(payload.get('companyId')) or "Not Available"
    org_id = bson.string_type(payload.get('organizationId')[0]) or "Not Available"
    is_active_record = 1
    valid_from_date = datetime.now().strftime("%Y/%m/%d, %H:%M:%S")
    valid_till_date = datetime.max.strftime("%Y/%m/%d, %H:%M:%S")
    valid_from_date_id = get_date_id_from_datetime(datetime.now())
    valid_till_date_id = get_date_id_from_datetime(datetime.max)

    row = {'strain_id': strain_id,
           'strain_created_on_date': strain_created_on_date,
           'strain_created_on_date_id': strain_created_on_date_id,
           'strain_name': strain_name, 'company_id': company_id,
           'strain_status': strain_status, 'indica_level': indica_level,
           'sativa_level': sativa_level,
           'tetra_hydro_cannabinol': tetra_hydro_cannabinol,
           'canna_bichromene': canna_bichromene,
           'tetra_hydro_cannabinolic': tetra_hydro_cannabinolic,
           'canna_bicyclol': canna_bicyclol, 'canna_bivarin': canna_bivarin,
           'canna_bodiolic_acide': canna_bodiolic_acide,
           'tetra_hydro_cannabi_varin': tetra_hydro_cannabi_varin,
           'canna_bi_diol': canna_bi_diol,
           'canna_bi_nol': canna_bi_nol, 'canna_bi_gerol': canna_bi_gerol,
           'org_id': org_id, 'is_active_record': is_active_record,
           'valid_from_date': valid_from_date,
           'valid_till_date': valid_till_date,
           'valid_from_date_id': valid_from_date_id,
           'valid_till_date_id': valid_till_date_id
           }

    return row

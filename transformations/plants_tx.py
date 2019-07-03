import bson
from .utils import calculate_days, get_date_id_from_datetime
from datetime import datetime


def create_from_mongo(payload):
    days_in_nursery = 0
    days_in_flowering = 0
    days_in_vegetative = 0
    flowering_entry_date = datetime.min.strftime("%Y/%m/%d, %H:%M:%S")
    flowering_entry_date_id = get_date_id_from_datetime(datetime.min)
    harvesting_date = datetime.min.strftime("%Y/%m/%d, %H:%M:%S")
    harvesting_date_id = get_date_id_from_datetime(datetime.min)
    vegetative_entry_date = datetime.min.strftime("%Y/%m/%d, %H:%M:%S")
    vegetative_entry_date_id = get_date_id_from_datetime(datetime.min)
    plant_id = bson.string_type(payload.get('_id'))
    company_id = bson.string_type(payload.get('companyId'))
    plant_batch_id = bson.string_type(payload.get('batchId')) or "NA"
    plantation_date = payload.get('createdOn')#.strftime("%Y/%m/%d, %H:%M:%S")
    plantation_date_id = get_date_id_from_datetime(datetime.strptime(payload.get('createdOn'), "%Y/%m/%d, %H:%M:%S"))
    actions = payload.get('actions')
    plant_status = "growing"
    growing_phase = "nursery"
    stage_destroyed_in = "not_destroyed"
    last_state = "nursery"
    harvested_output = 0
    total_waste = 0
    is_active_record = 1
    valid_from_date = datetime.now().strftime("%Y/%m/%d, %H:%M:%S")
    valid_till_date = datetime.max.strftime("%Y/%m/%d, %H:%M:%S")
    valid_from_date_id = get_date_id_from_datetime(datetime.now())
    valid_till_date_id = get_date_id_from_datetime(datetime.max)

    if actions:
        for act in actions:
            if act['action'] == 'Flowering':
                flowering_entry_date = act["actionOn"].strftime("%Y/%m/%d, %H:%M:%S")
                flowering_entry_date_id = get_date_id_from_datetime(act["actionOn"])
                days_in_nursery = calculate_days(plantation_date,
                                                 flowering_entry_date)

                growing_phase = act['action'].lower()
                if last_state == "nursery":
                    days_in_vegetative = 0
                    vegetative_entry_date = flowering_entry_date
                    #vegetative_entry_date_id = get_date_id_from_datetime()
                last_state = act['action'].lower()

            elif act['action'] == "Harvest":
                harvesting_date = act["actionOn"].strftime("%Y/%m/%d, %H:%M:%S")
                harvesting_date_id = get_date_id_from_datetime(act["actionOn"])
                days_in_flowering = calculate_days(flowering_entry_date,
                                                   harvesting_date)
                status = "harvested"
                growing_phase = act['action'].lower()
                last_state = act['action']

            elif act['action'] == 'Destroy':
                growing_phase = act['action'].lower()
                status = "destroyed"
                stage_destroyed_in = last_state

    if payload.get('manicures'):
        harvested_output = harvested_output + sum([manicure.get('weight') for manicure in payload.get("manicures")])

    if payload.get('harvestWeight'):
        harvested_output = harvested_output + payload.get("harvestWeight").get("weight")

    if payload.get('wastes'):
        total_waste = sum([waste.get('weight') for waste in payload.get("wastes")])

    row = {'company_id': company_id, 'plant_batch_id': plant_batch_id,
           'plant_id': plant_id, 'plant_status': plant_status, 'harvested_output': harvested_output,
           'days_in_nursery': days_in_nursery,
           'plantation_date': plantation_date,
           'plantation_date_id': plantation_date_id,
           'days_in_flowering': days_in_flowering,
           'flowering_entry_date': flowering_entry_date,
           'stage_destroyed_in': stage_destroyed_in, 'harvesting_date': harvesting_date,
           'total_waste': total_waste, 'days_in_vegetative': days_in_vegetative,
           'growing_phase': growing_phase, 'vegetative_entry_date': vegetative_entry_date,
           'is_active_record': is_active_record,
           'valid_from_date': valid_from_date, 'valid_till_date': valid_till_date,
           'valid_from_date_id': valid_from_date_id, 'valid_till_date_id': valid_till_date_id,
           'flowering_entry_date_id':flowering_entry_date_id, 'harvesting_date_id':harvesting_date_id,
           'vegetative_entry_date_id':vegetative_entry_date_id
           }
    print(row)
    return row


def plant_event_to_nursery(payload):
    days_in_nursery = 0
    days_in_flowering = 0
    days_in_vegetative = 0
    flowering_entry_date = None  # TODO: default date
    flowering_entry_date_id = ""  # get_date_id_from_datetime(flowering_entry_date)
    harvesting_date = None
    harvesting_date_id = ""  # get_date_id_from_datetime(harvesting_date)
    vegetative_entry_date = None
    vegetative_entry_date_id = ""  # get_date_id_from_datetime(vegetative_entry_date)
    company_id = bson.string_type(payload.get('companyId'))
    org_id = bson.string_type(payload.get('organization'))
    strain_id = bson.string_type(payload.get('strain'))
    plant_batch_id = bson.string_type(payload.get('_id'))
    plantation_date = payload.get('plantPhaseDetails')[0].get("lastPhaseTransitionDate")
    plantation_date_id = get_date_id_from_datetime(plantation_date)
    sub_inventory_id = payload.get('plantPhaseDetails')[0].get("subInventory")
    plant_status = "growing"
    growing_phase = "nursery"
    stage_destroyed_in = "not_destroyed"
    last_state = "Nursery"
    harvested_output = 0
    total_waste = 0
    is_active_record = 1
    valid_from_date = datetime.now()
    valid_till_date = datetime.max
    valid_from_date_id = get_date_id_from_datetime(valid_from_date)
    valid_till_date_id = get_date_id_from_datetime(valid_till_date)

    rows = list()
    for plant in payload.get("plantsDetails"):
        plant_id = bson.string_type(payload.get('_id'))

        row = {'company_id': company_id,
               'org_id': org_id,
               'strain_id': strain_id,
               'plant_batch_id': plant_batch_id,
               'plant_id': plant_id,
               'plant_status': plant_status,
               'growing_phase': growing_phase,
               'plantation_date': plantation_date,
               'plantation_date_id': plantation_date_id,
               'days_in_nursery': days_in_nursery,
               'days_in_vegetative': days_in_vegetative,
               'days_in_flowering': days_in_flowering,
               'vegetative_entry_date': vegetative_entry_date,
               'vegetative_entry_date_id': vegetative_entry_date_id,
               'flowering_entry_date': flowering_entry_date,
               'flowering_entry_date_id': flowering_entry_date_id,
               'harvesting_date': harvesting_date,
               'harvesting_date_id': harvesting_date_id,
               'stage_destroyed_in': stage_destroyed_in,
               'harvested_output': harvested_output,
               'total_waste': total_waste,
               'is_active_record': is_active_record,
               'valid_from_date': valid_from_date,
               'valid_till_date': valid_till_date,
               'valid_from_date_id': valid_from_date_id,
               'valid_till_date_id': valid_till_date_id
               }
        rows.append(row)

    return rows


def plant_to_vegetative(payload):
    rows = []
    for plant in payload.get("plants"):
        plant_id = plant.get('_id')
        growing_phase = "vegetative"
        vegetative_entry_date = plant.get('batch_data').get("plantPhaseDetails").get("lastPhaseTransitionDate")
        vegetative_entry_date_id = get_date_id_from_datetime(vegetative_entry_date)
        sub_inventory_id = plant.get('subInventoryId')
        is_active_record = 1
        valid_from_date = datetime.now()
        valid_till_date = datetime.max
        valid_from_date_id = get_date_id_from_datetime(valid_from_date)
        valid_till_date_id = get_date_id_from_datetime(valid_till_date)

        total_waste = 0

        if plant.get("wastes"):
            total_waste = sum([waste.get("weight") for waste in plant.get("wastes")])

        row = {'plant_id': plant_id, 'growing_phase': growing_phase,
               'vegetative_entry_date': vegetative_entry_date,
               'vegetative_entry_date_id': vegetative_entry_date_id,
               'sub_inventory_id': sub_inventory_id,
               'total_waste': total_waste,
               'is_active_record': is_active_record,
               'valid_from_date': valid_from_date,
               'valid_till_date': valid_till_date,
               'valid_from_date_id': valid_from_date_id,
               'valid_till_date_id': valid_till_date_id}
        rows.append(row)
    return rows


def plant_to_flowering(payload):
    rows = []
    for plant in payload.get("plants"):
        plant_id = plant.get('_id')
        growing_phase = "flowering"
        flowering_entry_date = plant.get('batch_data').get("plantPhaseDetails").get("lastPhaseTransitionDate")
        flowering_entry_date_id = get_date_id_from_datetime(flowering_entry_date)
        sub_inventory_id = plant.get('subInventoryId')
        total_waste = 0
        is_active_record = 1
        valid_from_date = datetime.now()
        valid_till_date = datetime.max
        valid_from_date_id = get_date_id_from_datetime(valid_from_date)
        valid_till_date_id = get_date_id_from_datetime(valid_till_date)

        if plant.get("wastes"):
            total_waste = sum([waste.get("weight") for waste in plant.get("wastes")])

        row = {'plant_id': plant_id, 'growing_phase': growing_phase,
               'flowering_entry_date': flowering_entry_date,
               'flowering_entry_date_id': flowering_entry_date_id,
               'sub_inventory_id': sub_inventory_id,
               'total_waste': total_waste,
               'is_active_record': is_active_record,
               'valid_from_date': valid_from_date,
               'valid_till_date': valid_till_date,
               'valid_from_date_id': valid_from_date_id,
               'valid_till_date_id': valid_till_date_id}
        rows.append(row)
    return rows


def plant_to_harvest(payload):
    rows = []
    for plant in payload.get("plants"):
        plant_id = plant.get('_id')
        growing_phase = "harvest"
        plant_status = "harvested"
        harvesting_date_id = plant.get('batch_data').get("plantPhaseDetails").get("lastPhaseTransitionDate")
        harvesting_date = get_date_id_from_datetime(harvesting_date)
        harvested_output = plant.get("harvestWeight").get("weight")
        sub_inventory_id = plant.get('subInventoryId')
        is_active_record = 1
        valid_from_date = datetime.now()
        valid_till_date = datetime.max
        valid_from_date_id = get_date_id_from_datetime(valid_from_date)
        valid_till_date_id = get_date_id_from_datetime(valid_till_date)

        total_waste = 0

        if plant.get("wastes"):
            total_waste = sum([waste.get("weight") for waste in plant.get("wastes")])

        row = {'plant_id': plant_id, 'growing_phase': growing_phase,
               'harvesting_date_id': harvesting_date_id,
               'harvesting_date': harvesting_date,
               'sub_inventory_id': sub_inventory_id,
               'plant_status': plant_status,
               'harvested_output': harvested_output,
               'total_waste': total_waste,
               'is_active_record': is_active_record,
               'valid_from_date': valid_from_date,
               'valid_till_date': valid_till_date,
               'valid_from_date_id': valid_from_date_id,
               'valid_till_date_id': valid_till_date_id}
        rows.append(row)
    return rows


def plant_to_manicure(payload):
    rows = []
    for plant in payload.get("plants"):
        plant_id = payload.get('_id')
        if payload.get('manicures'):
            harvested_output = sum([manicure.get('weight') for manicure in payload.get("manicures")])

        is_active_record = 1
        valid_from_date = datetime.now()
        valid_till_date = datetime.max
        valid_from_date_id = get_date_id_from_datetime(valid_from_date)
        valid_till_date_id = get_date_id_from_datetime(valid_till_date)

        row = {'plant_id': plant_id, 'harvested_output': harvested_output,
               'is_active_record': is_active_record,
               'valid_from_date': valid_from_date,
               'valid_till_date': valid_till_date,
               'valid_from_date_id': valid_from_date_id,
               'valid_till_date_id': valid_till_date_id}
        rows.append(row)
    return rows


def plant_to_destroy(payload):
    plant_id = payload.get('_id')
    growing_phase = "destroy"
    plant_status = "destroyed"
    is_active_record = 1
    valid_from_date = datetime.now()
    valid_till_date = datetime.max
    valid_from_date_id = get_date_id_from_datetime(valid_from_date)
    valid_till_date_id = get_date_id_from_datetime(valid_till_date)

    row = {'plant_id': plant_id, 'plant_status': plant_status,
           'growing_phase': growing_phase,
           'is_active_record': is_active_record,
           'valid_from_date': valid_from_date,
           'valid_till_date': valid_till_date,
           'valid_from_date_id': valid_from_date_id,
           'valid_till_date_id': valid_till_date_id}

    return row


def plant_relocation_event(payload):
    plant_id = payload.get('_id')
    sub_inventory_id = payload.get('subInventoryId')
    is_active_record = 1
    valid_from_date = datetime.now()
    valid_till_date = datetime.max
    valid_from_date_id = get_date_id_from_datetime(valid_from_date)
    valid_till_date_id = get_date_id_from_datetime(valid_till_date)

    row = {'plant_id': plant_id, 'sub_inventory_id': sub_inventory_id,
           'is_active_record': is_active_record,
           'valid_from_date': valid_from_date,
           'valid_till_date': valid_till_date,
           'valid_from_date_id': valid_from_date_id,
           'valid_till_date_id': valid_till_date_id}

    return row

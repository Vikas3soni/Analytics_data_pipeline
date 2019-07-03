
plants_batch_cache = dict()
plants_group_cache = dict()


def add_to_cache(plant_batch_id, plant_batch):
    plants_batch_cache[plant_batch_id] = plant_batch


def get_from_cache(plant_batch_id):
    return plants_batch_cache[plant_batch_id]


def add_group_inventory_to_cache(group_id, inventory):
    plants_group_cache[group_id] = inventory


def get_group_inventory_from_cache(group_id):
    return plants_group_cache[group_id]

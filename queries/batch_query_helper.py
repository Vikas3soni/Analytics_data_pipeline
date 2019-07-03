# get values of given keys from dictionary.
def get_values(data, keys):
    row = []
    for key in keys:
        row.append(data.get(key))
    return str(tuple(row))


# insert in bulk
def batch_insert_query(table_name, data):
    table_name = table_name
    keys = list(data[0].keys())
    print(keys)
    query_to_execute = """INSERT INTO {0}({1}) VALUES {2};""".format(
                       table_name, ", ".join(keys),
                       ", ".join([get_values(i, keys) for i in data]))
    print(query_to_execute)
    return query_to_execute


# get ids from rows
def get_ids_from_rows(data, id_field_name):
    ids = []
    for row in data:
        ids.append((row[id_field_name], 1))
    return str(tuple(ids))


# update entries in bult (use only if all set values are same.)
def batch_update_query(table_name, data, id_field_name):

    query_to_execute = """UPDATE {0} SET is_active_record=0,
                       valid_till_date='{1}', valid_till_date_id={2}
                       WHERE ({3}, is_active_record) IN{4};""".format(
                       table_name, data[0].get('valid_from_date'),
                       data[0].get('valid_from_date_id'),
                       id_field_name, get_ids_from_rows(data, id_field_name))

    return query_to_execute


# delete entries in bulk.
def batch_delete_query(table_name, data):
    query_to_execute = """UPDATE {0} SET is_active_record=0,
                       valid_till_date={1}, valid_till_date_id={2}
                       WHERE (id, is_active_record) IN{3};""".format(
                       table_name, data[0].get('valid_from_date'),
                       data[0].get('valid_from_date_id'),
                       get_ids_from_rows(data, "id"))

    return query_to_execute

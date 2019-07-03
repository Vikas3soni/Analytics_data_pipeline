from .utils import execute_sql_query
from .batch_query_helper import batch_insert_query, batch_update_query


def upsert(conn, row):
    table_name = "ITEMS"
    id_field_name = 'item_id'
    pre_fec = """SET SQL_SAFE_UPDATES = 0;"""
    update = batch_update_query(table_name, row, id_field_name)
    insert = batch_insert_query(table_name, row)
    execute_sql_query(conn,insert)
    return "Success"



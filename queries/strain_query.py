from .utils import execute_sql_query
from .batch_query_helper import batch_insert_query, batch_update_query

def upsert(conn, row):
    print(row)
    table_name = "STRAINS"
    id_field_name = 'strain_id'
    pre_fec = """SET SQL_SAFE_UPDATES = 0;"""
    update = batch_update_query(table_name, row, id_field_name)
    insert = batch_insert_query(table_name, row)
    execute_sql_query(conn, update)
    execute_sql_query(conn, insert)
    return "Success"

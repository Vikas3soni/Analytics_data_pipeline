from .utils import execute_sql_query
from .batch_query_helper import batch_insert_query, batch_update_query

def upsert(conn, row):
    table_name = "FINISHED_GOODS"
    id_field_name = 'fg_id'
    pre_fec = """SET SQL_SAFE_UPDATES = 0;"""
    update = batch_update_query(table_name, row, id_field_name)
    insert = batch_insert_query(table_name, row)
    execute_sql_query(conn, insert)
    return "Success"


def source_mapping(conn, row):
    query_to_execute = """
        SET SQL_SAFE_UPDATES = 0;
        DELETE FROM FG_SOURCE_MAP WHERE fg_id = %(fg_id)s;
        INSERT INTO FG_SOURCE_MAP(fg_id, fg_type, source_type,
            source_id, amount_used)
            VALUES (%(fg_id)s, %(fg_type)s, %(source_type)s,
            %(source_id)s, %(amount_used)s);"""

    execute_sql_query(conn, query_to_execute, row)

    return "Success"


def update_inventory(conn, row):
    query_to_execute = """
    SET SQL_SAFE_UPDATES = 0;

    SELECT available_sku_quantity, available_sku_count into @count, @quantity
        FROM ITEM_INVENTORY
        WHERE item_sku_id = %(item_sku_id)s and is_active_record = 1;

    UPDATE ITEM_INVENTORY SET is_active_record = 0,
            valid_till_date = %(valid_from_date)s,
            valid_till_date_id = '%(valid_from_date_id)s'
        WHERE item_sku_id = %(item_sku_id)s and is_active_record = 1;

    INSERT INTO ITEM_INVENTORY(item_sku_id, available_sku_count,
            available_sku_quantity,
            is_active_record, valid_from_date, valid_till_date,
            valid_from_date_id, valid_till_date_id)
        VALUES(%(item_sku_id)s, %(available_sku_count)s + @count,
            %(available_sku_quantity)s + @quantity,
            %(is_active_record)s, %(valid_from_date)s, %(valid_till_date)s,
            %(valid_from_date_id)s, %(valid_till_date_id)s);"""

    execute_sql_query(conn, query_to_execute, row)

    return "Success"

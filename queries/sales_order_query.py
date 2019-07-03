from .utils import execute_sql_query


def upsert(conn, row):
    """Fact Table"""
    query_to_execute = """ SET SQL_SAFE_UPDATES = 0;

    UPDATE ITEMS_INVENTORY SET is_active_record = 0,
            valid_till_date = '%(valid_from_date)s',
            valid_till_date_id = '%(valid_from_date_id)s'
        WHERE customer_id = %(customer_id)s and is_active_record = 1;

    INSERT INTO ITEMS_INVENTORY(order_system_id,order_id,customer_id,
            supplier_id, order_status, order_date, order_date_id,
            payment_method, org_id, company_id, total_amount_paid,
            total_amount_currency, discount, last_order_status_change_date,
            last_order_status_change_date_id, is_active_record,
            valid_from_date, valid_till_date, valid_from_date_id,
            valid_till_date_id)
        VALUES(%(order_system_id)s, %(order_id)s, %(customer_id)s,
            %(supplier_id)s, %(order_status)s, %(order_date)s,
            %(order_date_id)s, %(payment_method)s, %(org_id)s, %(company_id)s,
            %(total_amount_paid)s, %(total_amount_currency)s, %(discount)s,
            %(last_order_status_change_date)s,
            %(last_order_status_change_date_id)s,
            %(is_active_record)s, %(valid_from_date)s, %(valid_till_date)s,
            %(valid_from_date_id)s, %(valid_till_date_id)s)"""

    execute_sql_query(conn, query_to_execute, row)

    return "Success"

from .batch_query_helper import batch_insert_query, batch_update_query

def upsert(conn, row):
    table_name = "SALES_ORDERS"
    id_field_name = 'org_id'
    pre_fec = """SET SQL_SAFE_UPDATES = 0;"""
    update = batch_update_query(table_name, row, id_field_name)
    insert = batch_insert_query(table_name, row)
    execute_sql_query(conn, (pre_fec + update + insert))
    return "Success"


def sales_order_item_mapping(conn, row):
    """RelationShip Table - Map order id(s) against item id(s)."""
    query_to_execute = """
        SET SQL_SAFE_UPDATES = 0;
        DELETE FROM SALES_ORDER_ITEM_MAPPING WHERE order_id = %(order_id)s;
        INSERT INTO SALES_ORDER_ITEM_MAPPING (order_id, item_sku_id, item_id,
            amount_paid, item_sku_price, item_sku_currency, discount,
            item_sku_quantity_ordered)
        VALUES (%(order_id)s, %(item_sku_id)s, %(item_id)s, %(amount_paid)s,
            %(item_sku_price)s, %(item_sku_currency)s, %(discount)s,
            %(item_sku_quantity_ordered)s);"""

    execute_sql_query(conn, query_to_execute, row)

    return "Success"


def sales_order_fg_mapping(conn, row):
    """RelationShip Table - Map order id(s) against finished good id(s)."""
    query_to_execute = """
        SET SQL_SAFE_UPDATES = 0;
        DELETE FROM SALES_ORDER_FG_MAPPING WHERE order_id = %(order_id)s;
        INSERT INTO SALES_ORDER_FG_MAPPING (order_id, fg_id)
        VALUES (%(order_id)s, %(fg_id)s);"""

    execute_sql_query(conn, query_to_execute, row)

    return "Success"


def update_inventory(conn, row):
    """update inventory fact table from sales event"""
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
        VALUES(%(item_sku_id)s, @count - %(available_sku_count)s,
            @quantity - %(available_sku_quantity)s,
            %(is_active_record)s, %(valid_from_date)s, %(valid_till_date)s,
            %(valid_from_date_id)s, %(valid_till_date_id)s);"""

    execute_sql_query(conn, query_to_execute, row)

    return "Success"


def update_fg_table(conn, row):
    """update fg/lot fact table from sales event"""
    query_to_execute = """
    SET SQL_SAFE_UPDATES = 0;

    SELECT fg_id, fg_type, source_type, org_id,
            strain_id, company_id, item_id, item_sku_id, sub_inventory_id,
            package_tag_id, total_fg_weight, available_fg_weight,
            fg_created_date, fg_created_date_id, fg_expiry_date,
            fg_expiry_date_id, fg_status, item_sku_unit_quantity,
            item_sku_unit_count
        FROM FINISHED_GOODS_HISTORY into @fg_id, @fg_type, @source_type, @org_id,
            @strain_id, @company_id, @item_id, @item_sku_id, @sub_inventory_id,
            @package_tag_id, @total_fg_weight, @available_fg_weight,
            @fg_created_date, @fg_created_date_id, @fg_expiry_date,
            @fg_expiry_date_id, @fg_status, @item_sku_unit_quantity,
            @item_sku_unit_count
    WHERE fg_id = %(fg_id)s and is_active_record = 1;

    UPDATE FINISHED_GOODS SET is_active_record = 0,
            valid_till_date = '%(valid_from_date)s',
            valid_till_date_id = '%(valid_from_date_id)s'
        WHERE fg_id = %(fg_id)s and is_active_record = 1;

    INSERT INTO FINISHED_GOODS(fg_id, fg_type, source_type, org_id,
            strain_id, company_id, item_id, item_sku_id, sub_inventory_id,
            package_tag_id, total_fg_weight, available_fg_weight,
            fg_created_date, fg_created_date_id, fg_expiry_date,
            fg_expiry_date_id, fg_status, item_sku_unit_quantity,
            item_sku_unit_count, is_active_record, valid_from_date,
            valid_till_date, valid_from_date_id, valid_till_date_id)
        VALUES(@fg_id, @fg_type, @source_type, @org_id,
            @strain_id, @company_id, @item_id, @item_sku_id, @sub_inventory_id,
            @package_tag_id, @total_fg_weight, @available_fg_weight - %(used_fg_weight)s,
            @fg_created_date, @fg_created_date_id, @fg_expiry_date,
            @fg_expiry_date_id, @fg_status, @item_sku_unit_quantity,
            @item_sku_unit_count,
            %(is_active_record)s, %(valid_from_date)s, %(valid_till_date)s,
            %(valid_from_date_id)s, %(valid_till_date_id)s)
    """
    execute_sql_query(conn, query_to_execute, row)

    return "Success"

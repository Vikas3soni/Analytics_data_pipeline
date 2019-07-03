from .utils import execute_sql_query
from .batch_query_helper import batch_insert_query, batch_update_query


'''
def upsert(conn, row):
    query_to_execute = """
    SET SQL_SAFE_UPDATES = 0;

    UPDATE PLANTS SET is_active_record = 0,
            valid_till_date = '%(valid_from_date)s',
            valid_till_date_id = '%(valid_from_date_id)s'
        WHERE plant_id = %(plant_id)s and is_active_record = 1;

    INSERT INTO PLANTS(company_id, org_id, strain_id, plant_batch_id,
            plant_id, growing_phase, days_in_nursery, days_in_vegetative,
            days_in_flowering, plant_status, stage_destroyed_in,
            plantation_date, plantation_date_id,
            vegetative_entry_date_id, vegetative_entry_date,
            flowering_entry_date_id, flowering_entry_date, harvesting_date_id,
            harvesting_date, harvested_output, total_waste, is_active_record,
            valid_from_date, valid_till_date, valid_from_date_id,
            valid_till_date_id, sub_inventory_id)
        VALUES(%(company_id)s, %(org_id)s, %(strain_id)s, %(plant_batch_id)s,
            %(plant_id)s, %(growing_phase)s, %(days_in_nursery)s,
            %(days_in_vegetative)s, %(days_in_flowering)s, %(plant_status)s,
            %(stage_destroyed_in)s, %(plantation_date)s,
            %(plantation_date_id)s, %(vegetative_entry_date_id)s,
            %(vegetative_entry_date)s, %(flowering_entry_date_id)s,
            %(flowering_entry_date)s, %(harvesting_date_id)s,
            %(harvesting_date)s, %(harvested_output)s, %(total_waste)s,
            %(is_active_record)s,
            %(valid_from_date)s, %(valid_till_date)s, %(valid_from_date_id)s,
            %(valid_till_date_id)s, %(sub_inventory_id)s)"""

    execute_sql_query(conn, query_to_execute, row)
    return "Success"
    '''

def upsert(conn, row):
    print(row)
    table_name = "PLANTS"
    insert = batch_insert_query(table_name, row)

    query = insert
    execute_sql_query(conn, query)
    return "Success"


def insert_into_nursery(conn, row):
    query_to_execute = """
    SET SQL_SAFE_UPDATES = 0;

    UPDATE PLANTS SET is_active_record = 0,
            valid_till_date = '%(valid_from_date)s',
            valid_till_date_id = '%(valid_from_date_id)s'
        WHERE plant_id = %(plant_id)s and is_active_record = 1;

    INSERT INTO PLANTS(company_id, org_id, strain_id, plant_batch_id,
            plant_id, growing_phase, days_in_nursery, days_in_vegetative,
            days_in_flowering, plant_status, stage_destroyed_in,
            plantation_date, plantation_date_id,
            vegetative_entry_date_id, vegetative_entry_date,
            flowering_entry_date_id, flowering_entry_date, harvesting_date_id,
            harvesting_date, harvested_output, total_waste, is_active_record,
            valid_from_date, valid_till_date, valid_from_date_id,
            valid_till_date_id, sub_inventory_id)
        VALUES(%(company_id)s, %(org_id)s, %(strain_id)s, %(plant_batch_id)s,
            %(plant_id)s, %(growing_phase)s, %(days_in_nursery)s,
            %(days_in_vegetative)s, %(days_in_flowering)s, %(plant_status)s,
            %(stage_destroyed_in)s, %(plantation_date)s,
            %(plantation_date_id)s, %(vegetative_entry_date_id)s,
            %(vegetative_entry_date)s, %(flowering_entry_date_id)s,
            %(flowering_entry_date)s, %(harvesting_date_id)s,
            %(harvesting_date)s, %(harvested_output)s, %(total_waste)s,
            %(is_active_record)s,
            %(valid_from_date)s, %(valid_till_date)s, %(valid_from_date_id)s,
            %(valid_till_date_id)s, %(sub_inventory_id)s)"""

    execute_sql_query(conn, query_to_execute, row)

    return "Success"


def update_to_vegetative(conn, row):

    query_to_execute = """
        SET SQL_SAFE_UPDATES = 0;
        CREATE TEMPORARY TABLE temporary_table AS SELECT * FROM PLANTS
            WHERE plant_id=%(plant_id)s AND is_active_record=1;
        UPDATE temporary_table SET growing_phase=%(growing_phase)s,
                days_in_nursery = datediff(%(vegetative_entry_date)s,plantation_date),
                vegetative_entry_date=%(vegetative_entry_date)s,
                vegetative_entry_date_id=%(vegetative_entry_date_id)s,
                sub_inventory_id=%(sub_inventory_id)s,
                total_waste=%(total_waste)s + total_waste,
                is_active_record=%(is_active_record)s,
                valid_from_date=%(valid_from_date)s,
                valid_till_date=%(valid_till_date)s,
                valid_from_date_id=%(valid_from_date_id)s,
                valid_till_date_id=%(valid_till_date_id)s;
        UPDATE PLANTS SET is_active_record=0,
                valid_till_date=%(valid_from_date)s,
                valid_till_date_id=%(valid_from_date_id)s;
            WHERE plant_id=%(plant_id)s AND is_active_record=1;
        INSERT INTO PLANTS SELECT * FROM temporary_table
            WHERE plant_id=%(plant_id)s;
        DROP TABLE temporary_table; """

    execute_sql_query(conn, query_to_execute, row)

    return "Success"


def update_to_flowering(conn, row):

    query_to_execute = """
        SET SQL_SAFE_UPDATES = 0;
        CREATE TEMPORARY TABLE temporary_table AS SELECT * FROM PLANTS
            WHERE plant_id=%(plant_id)s AND is_active_record=1;
        UPDATE temporary_table SET flowering_entry_date = (%(flowering_entry_date)s),
                flowering_entry_date_id = (%(flowering_entry_date_id)s),
                vegetative_entry_date=CASE WHEN growing_phase = "vegetative" THEN vegetative_entry_date ELSE (%(flowering_entry_date)s) END,
                vegetative_entry_date_id=CASE WHEN growing_phase = "vegetative" THEN vegetative_entry_date_id ELSE (%(flowering_entry_date_id)s) END,
                days_in_vegetative = CASE WHEN growing_phase = "vegetative" THEN datediff(%(flowering_entry_date)s,vegetative_entry_date) ELSE 0 END,
                days_in_nursery = CASE WHEN growing_phase = "vegetative" THEN days_in_nursery ELSE datediff(%(flowering_entry_date)s,plantation_date) END,
                sub_inventory_id=%(sub_inventory_id)s,
                total_waste=%(total_waste)s + total_waste,
                growing_phase=%(growing_phase)s,
                is_active_record=%(is_active_record)s,
                valid_from_date=%(valid_from_date)s,
                valid_till_date=%(valid_till_date)s,
                valid_from_date_id=%(valid_from_date_id)s,
                valid_till_date_id=%(valid_till_date_id)s;                

        UPDATE PLANTS SET is_active_record=0,
                valid_till_date=%(valid_from_date)s,
                valid_till_date_id=%(valid_from_date_id)s;
            WHERE plant_id=%(plant_id)s AND is_active_record=1;
        INSERT INTO PLANTS SELECT * FROM temporary_table
            WHERE plant_id=%(plant_id)s;
        DROP TABLE temporary_table;
    """

    execute_sql_query(conn, query_to_execute, row)

    return "Success"


def update_to_harvest(conn, row):

    query_to_execute = """
    SET SQL_SAFE_UPDATES = 0;
        CREATE TEMPORARY TABLE temporary_table AS SELECT * FROM PLANTS
            WHERE plant_id=%(plant_id)s AND is_active_record=1;
        UPDATE temporary_table SET growing_phase=%(growing_phase)s,
                days_in_flowering = datediff(%(harvesting_date_id)s,flowering_entry_date_id),
                plant_status = (%(plant_status)s),
                harvested_output = (%(harvested_output)s) + harvested_output,
                harvesting_date = (%(harvesting_date)s),
                harvesting_date_id = (%(harvesting_date_id)s),
                sub_inventory_id=%(sub_inventory_id)s,
                total_waste=%(total_waste)s + total_waste,
                is_active_record=%(is_active_record)s,
                valid_from_date=%(valid_from_date)s,
                valid_till_date=%(valid_till_date)s,
                valid_from_date_id=%(valid_from_date_id)s,
                valid_till_date_id=%(valid_till_date_id)s;
        UPDATE PLANTS SET is_active_record=0,
                valid_till_date=%(valid_from_date)s,
                valid_till_date_id=%(valid_from_date_id)s;
            WHERE plant_id=%(plant_id)s AND is_active_record=1;
        INSERT INTO PLANTS SELECT * FROM temporary_table
            WHERE plant_id=%(plant_id)s;
        DROP TABLE temporary_table;
    """

    execute_sql_query(conn, query_to_execute, row)

    return "Success"


def update_to_manicure(conn, row):

    query_to_execute = """
        SET SQL_SAFE_UPDATES = 0;
        CREATE TEMPORARY TABLE temporary_table AS SELECT * FROM PLANTS
            WHERE plant_id=%(plant_id)s AND is_active_record=1;
        UPDATE temporary_table SET growing_phase=%(growing_phase)s,
                plant_status = (%(plant_status)s),
                harvested_output = (%(harvested_output)s) + harvested_output,
                total_waste=%(total_waste)s + total_waste,
                is_active_record=%(is_active_record)s,
                valid_from_date=%(valid_from_date)s,
                valid_till_date=%(valid_till_date)s,
                valid_from_date_id=%(valid_from_date_id)s,
                valid_till_date_id=%(valid_till_date_id)s;
        UPDATE PLANTS SET is_active_record=0,
                valid_till_date=%(valid_from_date)s,
                valid_till_date_id=%(valid_from_date_id)s;
            WHERE plant_id=%(plant_id)s AND is_active_record=1;
        INSERT INTO PLANTS SELECT * FROM temporary_table
            WHERE plant_id=%(plant_id)s;
        DROP TABLE temporary_table;
    """

    execute_sql_query(conn, query_to_execute, row)

    return "Success"


def update_to_destroy(conn, row):

    query_to_execute = """
        SET SQL_SAFE_UPDATES = 0;
        CREATE TEMPORARY TABLE temporary_table AS SELECT * FROM PLANTS
            WHERE plant_id=%(plant_id)s AND is_active_record=1;
        UPDATE temporary_table SET plant_status = (%(plant_status)s),
                stage_destroyed_in = growing_phase,
                growing_phase=%(growing_phase)s,
                is_active_record=%(is_active_record)s,
                valid_from_date=%(valid_from_date)s,
                valid_till_date=%(valid_till_date)s,
                valid_from_date_id=%(valid_from_date_id)s,
                valid_till_date_id=%(valid_till_date_id)s;
        UPDATE PLANTS SET is_active_record=0,
                valid_till_date=%(valid_from_date)s,
                valid_till_date_id=%(valid_from_date_id)s;
            WHERE plant_id=%(plant_id)s AND is_active_record=1;
        INSERT INTO PLANTS SELECT * FROM temporary_table
            WHERE plant_id=%(plant_id)s;
        DROP TABLE temporary_table;
        """

    execute_sql_query(conn, query_to_execute, row)

    return "Success"

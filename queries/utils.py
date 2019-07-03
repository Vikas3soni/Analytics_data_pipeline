def execute_sql_query(conn, query_to_execute, row= None):
    with conn.cursor() as cur:
        cur.execute(query_to_execute)
        conn.commit()

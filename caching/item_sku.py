import pandas as pd


columns = ['item_sku_id', 'item_id', 'item_sku_unit_quantity']
item_sku_df = pd.DataFrame(columns=columns)


def extract_id(item_id, item_sku_unit_quantity):
    value = item_sku_df.loc[(item_sku_df['item_sku_unit_quantity'] ==
                            item_sku_unit_quantity) &
                            (item_sku_df['item_id'] == item_id),
                            'item_sku_id'].values
    return value


def find_sku_id(item_id, item_sku_unit_quantity, conn):
    value = extract_id(item_id,item_sku_unit_quantity)
    print(value)
    if len(value) == 0:
        item_sku_df = pd.read_sql_query(
                    '''SELECT item_sku_id, item_id, item_sku_unit_quantity
                    FROM ITEMS''',
                    conn)
        try:
            value = extract_id(item_id, item_sku_unit_quantity)
            value = value[0]
            return value
        except Exception as e:
            return None
    return value[0]

from transformations import plants_tx, company_tx, organization_tx, strain_tx
from transformations import harvest_batch_tx, plant_batch_tx, fg_tx, subinventory_tx
from transformations import customer_tx, item_inventory, item_tx, sales_order_tx
from queries import company_query, cutomers_query, fg_query, harvest_batch_query
from queries import item_inventory_query, item_query, organization_query, plant_batch_queries
from queries import plants_query, sales_order_query, strain_query, sub_inventory_query
from common import app_logger

logger = app_logger.setup_logger('event-router')


def router(conn, event_type, payload): 
    status = None

    if event_type == 'PLANT_BATCH_EVENT':
        logger.info("processing {0} event".format(event_type))
        try:
            row = plant_batch_tx.create(payload)
            status = plant_batch_queries.upsert(conn, row)
        except Exception as e:
            logger.error("Failed to process plant batch. id:{0} , error: {1}".format(payload.get('_id'), str(e)))
            raise Exception("Failed to process plant batch.")
        try:
            rows = plants_tx.plant_to_nursery(payload)
            status = plants_query.insert_into_nursery(conn, rows)
        except Exception as e:
            logger.error("Failed to plants into nursury phase. error: {0}".format(str(e)))
            raise Exception("Failed to plants into nursury phase.")
        return {"status": status}

    elif event_type == 'PLANT_VEGETATIVE_PHASE':
        logger.info("processing {0} event".format(event_type))
        try:
            rows = plants_tx.plant_to_vegetative(payload)
            status = plants_query.update_to_vegetative(conn, rows)
        except Exception as e:
            logger.error("Failed to process plant in vegetative phase. Error: {0}".format(str(e)))
            raise Exception("Failed to process plant in vegetative phase.")
        return {"status": status}

    elif event_type == 'PLANT_FLOWERING_PHASE':
        logger.info("processing {0} event".format(event_type))
        try:
            rows = plants_tx.plant_to_flowering(payload)
            status = plants_query.update_to_flowering(conn, rows)
        except Exception as e:
            logger.error("Failed to process plant in flowering phase. Error: {0}".format(str(e)))
            raise Exception("Failed to process plant in flowering phase.")
        return {"status": status}

    elif event_type == 'PLANT_HARVEST_PHASE':
        logger.info("processing {0} event".format(event_type))
        try:
            rows = plants_tx.plant_to_harvest(payload)
            status = plants_query.update_to_harvest(conn, rows)
        except Exception as e:
            logger.error("Failed to process plant in harvest phase. Error: {0}".format(str(e)))
            raise Exception("Failed to process plant in harvest phase.")
        return {"status": status}

    elif event_type == 'PLANT_MANICURE_PHASE':
        logger.info("processing {0} event".format(event_type))
        try:
            rows = plants_tx.plant_to_manicure(payload)
            status = plants_query.update_to_manicure(conn, rows)
        except Exception as e:
            logger.error("Failed to process plant in manicure phase. Error: {0}".format(str(e)))
            raise Exception("Failed to process plant in manicure phase.")
        return {"status": status}

    elif event_type == 'PLANT_DESTROY_PHASE':
        logger.info("processing {0} event".format(event_type))
        try:
            rows = plants_tx.plant_to_destroy(payload)
            status = plants_query.update_to_destroy(conn, rows)
        except Exception as e:
            logger.error("Failed to process plant in destroy phase. Error: {0}".format(str(e)))
            raise Exception("Failed to process plant in destroy phase.")
        return {"status": status}

    elif event_type == 'LOT_DATA_EVENT':
        logger.info("processing {0} event".format(event_type))
        try:
            row = fg_tx.create(payload, "lot", "package")
            status = fg_query.upsert(conn, row)
        except Exception as e:
            logger.error("Failed to process lot. id:{0}, error: {1}".format(payload.get('_id'), str(e)))
            raise Exception("Failed to process lot.")

        try:
            row = fg_tx.fg_source_table_update(payload, "lot", "package")
            status = fg_query.source_mapping(conn, row)
        except Exception as e:
            logger.error("Failed to map lot and source. id: {0}, error: {1}".format(payload.get('_id'), str(e)))
            raise Exception("Failed to map lot and source.")
        
        try:
            row = fg_tx.item_inventory_table_update(payload)
            status = fg_query.update_inventory(conn, row)
        except Exception as e:
            logger.error("Failed toupdate inventory. id: {0}, error: {1}".format(payload.get('_id'), str(e)))
            raise Exception("Failed toupdate inventory.")
        return {"status": status}

    elif event_type == 'PLANT_MATERIAL_EVENT':
        logger.info("processing {0} event".format(event_type))
        try:
            row = fg_tx.create(payload, "package", "harvest_batch")
            status = fg_query.upsert(conn, row)
        except Exception as e:
            logger.error("Failed to process lot. id: {0}, error: {1}".format(payload.get('_id'), str(e)))
            raise Exception("Failed to process lot.")
        try:
            row = fg_tx.fg_source_table_update(payload, "package", "harvest_batch")
            status = fg_query.source_mapping(conn, row)
        except Exception as e:
            logger.error("Failed to map package and source. id: {0}, error: {1}".format(payload.get('_id'), str(e)))
            raise Exception("Failed to map package and source.")
        try:
            row = fg_tx.item_inventory_table_update(payload)
            status = fg_query.update_inventory(conn, row)
        except Exception as e:
            logger.error("Failed to update inventory. id: {0}, error: {1}".format(payload.get('_id'), str(e)))
            raise Exception("Failed to update inventory.")
        return {"status": status}

    elif event_type == 'HARVEST_CREATE_EVENT':
        logger.info("processing {0} event".format(event_type))
        try:
            row = harvest_batch_tx.harvest_batch_create(payload)
            status = harvest_batch_query.insert_new_harvest_batch(conn, row)
        except Exception as e:
            logger.error("Failed to process harvest batch. id: {0}, error: {1}".format(payload.get('_id'), str(e)))
            raise Exception("Failed to process harvest batch.")
        return {"status": status}

    elif event_type == 'COMPANY_EVENT':
        logger.info("processing {0} event".format(event_type))
        try:
            row = company_tx.create(payload)
            status = company_query.upsert(conn, row)
        except Exception as e:
            logger.error("Failed to process company. id: {0}, error: {1}".format(payload.get('_id'), str(e)))
            raise Exception("Failed to process company.")
        return {"status": status}

    elif event_type == 'SUB_INVENTORY_EVENT':
        logger.info("processing {0} event".format(event_type))
        try:
            row = sub_inventory_query.create(payload)
            status = sub_inventory_query.upsert(conn, row)
        except Exception as e:
            logger.error("Failed to process sub inventory. id: {0}, error: {1}".format(payload.get('_id'), str(e)))
            raise Exception("Failed to process sub inventory.")
        return {"status": status}

    elif event_type == 'ORGANIZATION_EVENT':
        logger.info("processing {0} event".format(event_type))
        try:
            row = organization_tx.create(payload)
            status = organization_query.upsert(conn, row)
        except Exception as e:
            logger.error("Failed to process organization. id: {0}, error: {1}".format(payload.get('_id'), str(e)))
            raise Exception("Failed to process organization.")
        return {"status": status}

    elif event_type == 'STRAIN_DATA_EVENT':
        logger.info("processing {0} event".format(event_type))
        try:
            row = strain_tx.create(payload)
            status = strain_query.upsert(conn, row)
        except Exception as e:
            logger.error("Failed to process strain. id: {0}, error: {1}".format(payload.get('_id'), str(e)))
            raise Exception("Failed to process strain.")
        return {"status": status}

    elif event_type == 'ITEM_DATA_EVENT':
        payload = event.get('itemDetails')
        logger.info("processing {0} event".format(event_type))
        try:
            row = item_tx.create(payload)
            status = item_query.upsert(conn, row)
        except Exception as e:
            logger.error("Failed to process plant batch. idL {0}, error: {1}".format(payload.get('_id'), str(e)))
            raise Exception("Failed to process plant batch.")
        return {"status": status}

    elif event_type == 'ORDER_EVENT':
        logger.info("processing {0} event".format(event_type))
        try:
            row = sales_order_tx.create(payload)
            status = sales_order_query.upsert(conn, row)
        except Exception as e:
            logger.error("Failed to process sales order. id: {0}, error: {1}".format(payload.get('_id'), str(e)))
            raise Exception("Failed to process sales order.")
        try:
            row = sales_order_tx.fg_update(payload)
            status = sales_order_query.update_fg_table(conn, row)
        except Exception as e:
            logger.error("Failed to update finished goods table. id: {0}, error: {1}".format(payload.get('_id'), str(e)))
            raise Exception("Failed to update finished goods table.")
        try:
            row = sales_order_tx.item_inventory_update(payload)
            status = sales_order_query.update_inventory(conn, row)
        except Exception as e:
            logger.error("Failed to update inventory. id: {0}, error: {1}".format(payload.get('_id'), str(e)))
            raise Exception("Failed to update inventory.")
        try:
            row = sales_order_tx.item_order_map(payload)
            status = sales_order_query.sales_order_item_mapping(conn, row)
        except Exception as e:
            logger.error("Failed to map order with item. id: {0}, error: {1}".format(payload.get('_id'), str(e)))
            raise Exception("Failed to map order with item.")
        try:
            row = sales_order_tx.sales_order_fg_map(payload)
            status = sales_order_query.sales_order_fg_mapping(conn, row)
        except Exception as e:
            logger.error("Failed to map order with fg. id: {0}, error: {1}".format(payload.get('_id'), str(e)))
            raise Exception("Failed to map order with fg.")
        return {"status": status}

    else:
        logger.info("Unknown event received: {0}".format(event_type))
        return {"status": "unknown event"}

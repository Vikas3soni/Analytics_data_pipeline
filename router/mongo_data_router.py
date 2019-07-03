import os
from common import app_logger
from transformations import company_tx, organization_tx, strain_tx, plants_tx, item_tx
from transformations import plant_batch_tx, subinventory_tx, harvest_batch_tx, fg_tx
from transformations import sales_order_tx,  customer_tx
from queries import company_query, customers_query, fg_query, harvest_batch_query, item_query, organization_query
from queries import plant_batch_queries, plants_query, sales_order_query, strain_query, sub_inventory_query
from caching import plant_batch_cache

import bson

logger = app_logger.setup_logger('mongo-data-router')

envirn = os.getenv("ENVIRONMENT")
batch_counter = 1000


def companies(conn, company_collection):
    rows = []
    i = 0
    companies = company_collection.find()
    total_count = companies.count()
    for company in companies:
        i += 1
        try:
            logger.info("Transforming Company with id: {0}".format(company.get('_id')))
            rows.append(company_tx.create(company))
            logger.info("Done")
        except Exception as e:
            logger.error("Failed to Transform company, error: {0}".format(str(e)))
        if i > batch_counter or i == total_count:
            total_count = total_count - batch_counter
            i = 0
            try:
                status = company_query.upsert(conn, rows)
                rows = []
            except Exception as e:
                logger.error("Failed to insert companies, error: {0}".format(str(e)))


def organizations(conn, organizations_collection):
    rows = []
    i = 0
    organizations = organizations_collection.find()
    total_count = organizations.count()
    for organization in organizations:
        if i >1000:
            break
        i += 1
        try:
            logger.info("Transforming Organization with id: {0}".format(organization.get('_id')))
            rows.append(organization_tx.create(organization))
        except Exception as e:
            logger.error("Failed to Transform Organization, error: {0}".format(str(e)))
        if i > batch_counter or i == total_count:
            total_count = total_count - batch_counter
            i = 0
            try:
                status = organization_query.upsert(conn, rows)
                rows = []
            except Exception as e:
                logger.error("Failed to insert Organization, error: {0}".format(str(e)))


def strains(conn, strains_collection):
    rows = []
    i = 0
    strains = strains_collection.find()
    total_count = strains.count()
    for strain in strains:
        i += 1
        try:
            logger.info("Transforming strain with id: {0}".format(strain.get('_id')))
            row = strain_tx.create(strain)
            rows.append(row)
        except Exception as e:
            logger.error("Failed to Transform strain, error: {0}".format(str(e)))
        if i > batch_counter or i == total_count:
            total_count = total_count - batch_counter
            i = 0
            try:
                status = strain_query.upsert(conn, rows)
                rows = []
            except Exception as e:
                logger.error("Failed to insert strain, error: {0}".format(str(e)))


def plant_batches(conn, plant_batches_collection):
    rows = []
    i = 0
    plant_batches = plant_batches_collection.find()
    total_count = plant_batches.count()
    for plant_batch in plant_batches:
        i += 1
        try:
            logger.info("Transforming plant batch with id: {0}".format(plant_batch.get('_id')))
            for phase_group in plant_batch.get("plantPhaseDetails"):
                plant_batch_cache.add_group_inventory_to_cache(bson.string_type(phase_group.get("_id")),
                                                               phase_group.get("subInventory"))

            row = plant_batch_tx.create(plant_batch)
            rows.append(row)
            plant_batch_cache.add_to_cache(row['plant_batch_id'], row)
        except Exception as e:
            logger.error("Failed to Transform plant batch, error: {0}".format(str(e)))
        if i > batch_counter or i == total_count:
            total_count = total_count - batch_counter
            i = 0
            try:
                status = plant_batch_queries.upsert(conn, rows)
                #rows = []
            except Exception as e:
                logger.error("Failed to insert plant batch, error: {0}".format(str(e)))


def sub_inventory(conn, inventory_collection):
    rows = []
    i = 0
    sub_inventories = inventory_collection.find()
    total_count = sub_inventories.count()
    for sub_inventory in sub_inventories:
        i += 1
        try:
            logger.info("Transforming sub inventory with id: {0}".format(sub_inventory.get('_id')))
            rows.append(subinventory_tx.create(sub_inventory))
        except Exception as e:
            logger.error("Failed to Transform sub inventory, error: {0}".format(str(e)))
            if i > batch_counter or i == total_count:
                total_count = total_count - batch_counter
                i = 0
                try:
                    status = sub_inventory_query.upsert(conn, rows)
                    rows = []
                except Exception as e:
                    logger.error("Failed to insert sub inventory, error: {0}".format(str(e)))


def plants(conn, plants_collection):
    rows = []
    i = 1
    plants = plants_collection.find()
    total_count = plants.count()
    for plant in plants:
        i += 1
        try:
            logger.info("Transforming plant with id: {0}".format(plant.get('_id')))
            plant_batch = plant_batch_cache.get_from_cache(bson.string_type(plant.get('batchId')))
            plant['createdOn'] = plant_batch.get('plant_batch_created_on_date')
            row = plants_tx.create_from_mongo(plant)
            row['org_id'] = plant_batch.get('org_id')
            row['strain_id'] = plant_batch.get('strain_id')
            row['sub_inventory_id'] = plant_batch_cache.get_group_inventory_from_cache(plant.get("groupId")) or "Not Available"
            rows.append(row)
        except Exception as e:
             logger.error("Failed to Transform plant, error: {0}".format(str(e)))
        if i > batch_counter or i == total_count:
            total_count = total_count - batch_counter
            i = 0
            try:
                status = plants_query.upsert(conn, rows)
                rows = []
            except Exception as e:
                logger.error("Failed to insert plant, error: {0}".format(str(e)))


def salesorder(conn, sales_collection):
    """"once a sales order event comes inventory fact table, fg table,
    relationship table, sales order item table gets updated """
    sales_order_rows = []
    fg_update_rows = []
    item_inventory_update_rows = []
    order_fg_map_rows = []
    order_item_map_rows = []
    i = 0
    sales_orders = sales_collection.find()
    total_count = sales_orders.count()
    for sales_order in sales_orders:
        i += 1
        try:
            logger.info("Transforming sales order with id: {0}".format(sales_order.get('_id')))

            sales_order_rows.append(sales_order_tx.create(sales_order))
            fg_update_rows.extend(sales_order_tx.fg_update(sales_order))
            item_inventory_update_rows.extend(sales_order_tx.item_inventory_update(sales_order))
            order_fg_map_rows.extend(sales_order_tx.sales_order_fg_map(sales_order))
            order_item_map_rows.extend(sales_order_tx.item_order_map(sales_order))

        except Exception as e:
            logger.error("Failed to Transform sales order, error: {0}".format(str(e)))
        if i > batch_counter or i == total_count:
            total_count = total_count - batch_counter
            i = 0
            try:
                status = sales_order_query.upsert(conn, sales_order_rows)
                status = sales_order_query.update_fg_table(conn, fg_update_rows)
                status = sales_order_query.update_inventory(conn, item_inventory_update_rows)
                status = sales_order_query.sales_order_fg_mapping(conn, order_fg_map_rows)
                status = sales_order_query.sales_order_item_mapping(conn, order_item_map_rows)

                sales_order_rows = []
                fg_update_rows = []
                item_inventory_update_rows = []
                order_fg_map_rows = []
                order_item_map_rows = []
            except Exception as e:
                logger.error("Failed to insert sales order, error: {0}".format(str(e)))


def item(conn, item_collection):
    rows = []
    i = 0
    items = item_collection.find({"createdBy.name": { '$in':  ["Ganesh Patel", "vikas s"]}})
    total_count = items.count()
    for item in items:
        i += 1
        try:
            logger.info("Transforming item with id: {0}".format(item.get('_id')))
            rows.extend(item_tx.create(item))
        except Exception as e:
            logger.error("Failed to Transform item, error: {0}".format(str(e)))
        if i > batch_counter or i == total_count:
            total_count = total_count - batch_counter
            i = 0
            try:
                status = item_query.upsert(conn, rows)
                rows = []
            except Exception as e:
                logger.error("Failed to insert item, error: {0}".format(str(e)))


def customers(conn, customer_collection):
    rows = []
    i = 0
    customers = customer_collection.find()
    total_count = customers.count()
    for customer in customers:
        i += 1
        try:
            logger.info("Transforming customer with id: {0}".format(item.get('_id')))
            rows.append(customer_tx.create(customer))
        except Exception as e:
            logger.error("Failed to Transform customer, error: {0}".format(str(e)))
        if i > batch_counter or i == total_count:
            total_count = total_count - batch_counter
            i = 0
            try:
                status = customers_query.upsert(conn, rows)
                rows = []
            except Exception as e:
                logger.error("Failed to insert customer, error: {0}".format(str(e)))


def harvestbatch(conn, harvest_collection):
    rows = []
    i = 0
    harvest_batches = harvest_collection.find()
    total_count = harvest_batches.count()
    for harvest_batch in harvest_batches:
        i += 1
        print(harvest_batch)
        try:
            logger.info("Transforming harvest_batch with id: {0}".format(item.get('_id')))
            rows.append(harvest_batch_tx.create(harvest_batch))
        except Exception as e:
            logger.error("Failed to Transform harvest_batch, error: {0}".format(str(e)))
            if i > batch_counter or i == total_count:
                total_count = total_count - batch_counter
                i = 0
                try:
                    status = harvest_batch_query.upsert(conn, rows)
                    rows = []
                except Exception as e:
                    logger.error("Failed to insert harvest_batch, error: {0}".format(str(e)))


def lot(conn, fg_collection):
    finished_goods_rows = []
    fg_source_map_rows = []
    item_inventory_update_rows = []
    i = 0
    fgs = fg_collection.find({"createdBy.name": { '$in':  ["Ganesh Patel", "vikas s"]}})
    total_count = fgs.count()
    for fg in fgs:
        i += 1
        #try:
        #logger.info("Transforming lot with id: {0}".format(fg.get('_id')))
        print("inside lot router fun")
        fg_source_map_rows.extend(fg_tx.fg_source_table_update(fg, "lot", "package"))
        item_inventory_update_rows.append(fg_tx.item_inventory_table_update(fg))
        finished_goods_rows.append(fg_tx.create(fg, "lot", "package",conn))
        #except Exception as e:
        logger.error("Failed to Transform lot, error: {0}".format(str(e)))
        if i > batch_counter or i == total_count:
            total_count = total_count - batch_counter
            i = 0
            #try:
            status = fg_query.upsert(conn, finished_goods_rows)
            status = fg_query.source_mapping(conn, fg_source_map_rows)
            status = fg_query.update_inventory(conn, item_inventory_update_rows)

            finished_goods_rows = []
            fg_source_map_rows = []
            item_inventory_update_rows = []
            #except Exception as e:
             #   logger.error("Failed to insert lot, error: {0}".format(str(e)))


def package(conn, fg_collection):
    """"once a package event comes inventory fact table,
    relationship table gets updated """
    finished_goods_rows = []
    fg_source_map_rows = []
    item_inventory_update_rows = []
    i = 0
    fgs = fg_collection.find()
    total_count = fgs.count()
    for fg in fgs:
        i += 1
        try:
            logger.info("Transforming package with id: {0}".format(fg.get('_id')))

            finished_goods_rows.append(fg_tx.create(fg, "package", "harvest_batch"))
            fg_source_map_rows.extend(fg_tx.fg_source_table_update(fg, "package", "harvest_batch"))
            item_inventory_update_rows.append(fg_tx.item_inventory_table_update(fg))

        except Exception as e:
            logger.error("Failed to Transform package, error: {0}".format(str(e)))
        if i > batch_counter or i == total_count:
            total_count = total_count - batch_counter
            i = 0
            try:
                status = fg_query.upsert(conn, finished_goods_rows)
                status = fg_query.source_mapping(conn, fg_source_map_rows)
                status = fg_query.update_inventory(conn, item_inventory_update_rows)

                finished_goods_rows = []
                fg_source_map_rows = []
                item_inventory_update_rows = []
            except Exception as e:
                logger.error("Failed to insert package, error: {0}".format(str(e)))

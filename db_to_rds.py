import json
import pymysql
from common import app_logger
from common import configs
from common import connections
from router import mongo_data_router

logger = app_logger.setup_logger('DB_TO_RDS')

MONGO_HOST_NAME = configs.MONGO_HOST_NAME
MONGO_PORT = configs.MONGO_PORT
MONGO_USERNAME = configs.MONGO_USERNAME
MONGO_PASS = configs.MONGO_PASS
ERP_DB_NAME = configs.ERP_DB_NAME
ORDERS_DB_NAME = configs.ORDERS_DB
RDS_HOST_NAME = configs.RDS_HOST_NAME
RDS_USERNAME = configs.RDS_USERNAME
RDS_PASSWORD = configs.RDS_PASSWORD
AUTH_SOURCE_DB = configs.AUTH_SOURCE_DB
RDS_DB_NAME = configs.RDS_DB_NAME
COMPANY_COLLECTION = configs.COMPANY_COLLECTION
ORGANIZATION_COLLECTION = configs.ORGANIZATION_COLLECTION
STRAIN_COLLECTION = configs.STRAIN_COLLECTION
PLANT_BATCH_COLLECTION = configs.PLANT_BATCH_COLLECTION
PLANT_COLLECTION = configs.PLANT_COLLECTION
HARVEST_MATERIAL_COLLECTION = configs.HARVEST_MATERIAL_COLLECTION
SUB_INVENTORIES_COLLECTION = configs.SUB_INVENTORIES_COLLECTION
LOT_COLLECTION = configs.LOT_COLLECTION
PLANT_MATERIAL_COLLECTION = configs.PLANT_MATERIAL_COLLECTION
ITEMS_COLLECTION = configs.ITEMS_COLLECTION
CUSTOMERS_COLLECTION = configs.CUSTOMERS_COLLECTION
ORDERS_DB = configs.ORDERS_DB
ORDERS_COLLECTION = configs.ORDERS_COLLECTION


def processing():
    mongo_conn = connections.mongo_connection(MONGO_HOST_NAME, MONGO_PORT,
                                                  MONGO_USERNAME, MONGO_PASS,
                                                  AUTH_SOURCE_DB)
    erp_db = mongo_conn[ERP_DB_NAME]
    logger.info("Connected to Mongo db")
    rds_conn = connections.rds_connection(RDS_HOST_NAME, RDS_USERNAME,
                                          RDS_PASSWORD, RDS_DB_NAME, 15)


    # 
    #mongo_data_router.companies(rds_conn, erp_db[COMPANY_COLLECTION])
    # 
    #mongo_data_router.organizations(rds_conn, erp_db[ORGANIZATION_COLLECTION])
    # 
    #mongo_data_router.strains(rds_conn, erp_db[STRAIN_COLLECTION])
    # 
    #mongo_data_router.plant_batches(rds_conn, erp_db[PLANT_BATCH_COLLECTION])
    # 
    #mongo_data_router.plants(rds_conn, erp_db[PLANT_COLLECTION])
    #
    #mongo_data_router.harvestbatch(rds_conn, erp_db[HARVEST_MATERIAL_COLLECTION])
    #
    #mongo_data_router.sub_inventory(rds_conn, erp_db[SUB_INVENTORIES_COLLECTION])
    #
    mongo_data_router.item(rds_conn, erp_db[ITEMS_COLLECTION])
    #
    print("--------------------")
    print("item_complete")
    mongo_data_router.lot(rds_conn, erp_db[LOT_COLLECTION])
    #
    #mongo_data_router.package(rds_conn, erp_db[PLANT_MATERIAL_COLLECTION])
    #

    #
    #mongo_data_router.customers(rds_conn, erp_db[CUSTOMERS_COLLECTION])

    #order_db = mongo_conn[ORDERS_DB]
    #mongo_data_router.customers(rds_conn, order_db[ORDERS_COLLECTION])

    mongo_conn.close()
    rds_conn.close()
    return None


processing()

from common import app_logger
import os
import sys

logger = app_logger.setup_logger('configs')

MONGO_HOST_NAME = "13.234.247.98"
if len(MONGO_HOST_NAME) == 0:
    logger.error("MONGO_HOST_NAME is empty.")
    sys.exit()

MONGO_PORT = "27490"
if len(MONGO_PORT) == 0:
    logger.error("MONGO_PORT is empty.")
    sys.exit()
MONGO_PORT = int(MONGO_PORT)

MONGO_USERNAME = ""


MONGO_PASS = ""


RDS_HOST_NAME = "analytics-crelia-dev.c4fytxwsxgpp.ap-south-1.rds.amazonaws.com"
if len(RDS_HOST_NAME) == 0:
    logger.error("RDS_HOST_NAME is empty.")
    sys.exit()

RDS_USERNAME = "creliaanalytics"
if len(RDS_USERNAME) == 0:
    logger.error("RDS_USERNAME is empty.")
    sys.exit()

RDS_PASSWORD = "CreliaAnalytics"
if len(RDS_PASSWORD) == 0:
    logger.error("RDS_PASSWORD is empty.")
    sys.exit()

AUTH_SOURCE_DB = ""


RDS_DB_NAME = "cob_test_v1"
if len(RDS_DB_NAME) == 0:
    logger.error("RDS_DB_NAME is empty.")
    sys.exit()

MONGO_DB_NAME ="supplier_production"
if len(MONGO_DB_NAME) == 0:
    logger.error("MONGO_DB_NAME is empty.")
    sys.exit()

ERP_DB_NAME =  "supplier_production"


ORDERS_DB = "orders"
if len(ORDERS_DB) == 0:
    logger.error("ORDERS_DB is empty.")
    sys.exit()

COMPANY_COLLECTION = "companies"
ORGANIZATION_COLLECTION = "organizations"
STRAIN_COLLECTION = "strains"
PLANT_BATCH_COLLECTION = "batches"
PLANT_COLLECTION = "plants"
HARVEST_MATERIAL_COLLECTION = "harvestmaterials"
SUB_INVENTORIES_COLLECTION = "subinventories"
LOT_COLLECTION = "lots"
PLANT_MATERIAL_COLLECTION = "plantmaterials"
ITEMS_COLLECTION = "items"
CUSTOMERS_COLLECTION = "BusinessCustomers"
ORDERS_DB = "erp-orders"
ORDERS_COLLECTION = "erp-orders"

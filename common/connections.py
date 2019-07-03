from pymongo import MongoClient
import pymysql
from common import app_logger

logger = app_logger.setup_logger('connections')


def mongo_connection(mongo_host, port, username, password, auth_db):
    try:
        mongo_conn = MongoClient(host=mongo_host, port=port, username=username,
                                 password=password, authSource=auth_db)
        logger.info("Connected to Mongo Instance")
    except Exception as e:
        logger.error("Failed to connect to mongo: , error: {0}".format(str(e)))
        raise Exception("Failed to connect to mongo.")
    return mongo_conn


def rds_connection(host_name, username, password, db_name, connection_timeout):
    try:
        conn = pymysql.connect(host_name, user=username, passwd=password,
                               db=db_name, connect_timeout=connection_timeout)
        logger.info("Connected to RDS Instance")
    except Exception as e:
        logger.error("Failed to connect to mysql rds, error: {0}".format(str(e)))
        raise Exception("Failed to connect to RDS.")
    return conn

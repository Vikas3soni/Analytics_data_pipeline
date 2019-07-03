import json
import pymysql
from common import app_logger
from common import configs
from common import connections
from router import events_router

logger = app_logger.setup_logger('main')

RDS_HOST_NAME = configs.RDS_HOST_NAME
RDS_USERNAME = configs.RDS_USERNAME
RDS_PASSWORD = configs.RDS_PASSWORD
DB_NAME = configs.RDS_DB_NAME


def lambda_handler(event, context):
    conn = connections.rds_connection(RDS_HOST_NAME, RDS_USERNAME,
                                      RDS_PASSWORD, DB_NAME, 5)
    # if error then return from here and raise exception.
    # for record in event['Records']:
    #     # Kinesis data is base64 encoded so decode here
    #     kinesis_payload = base64.b64decode(record["kinesis"]["data"])
    #     print("Decoded payload: " + str(kinesis_payload))

    logger.info('New event received: {0}'.format(event["eventType"]))

    response = events_router(conn, event)
    logger.info('Response Status: {0}'.format(response['status']))
    conn.close()
    return None

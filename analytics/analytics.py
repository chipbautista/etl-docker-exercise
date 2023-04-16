from loguru import logger

import etl
from db import get_psql_engine, get_mysql_engine, get_analytics_table
from utils import wait_for_data


# prepare db engines & analytics table
psql_engine = get_psql_engine()
mysql_engine = get_mysql_engine()
analytics_table = get_analytics_table(mysql_engine)

# wait for other script to populate the database
wait_for_data(psql_engine)
logger.info("ETL starting...")

# etl
data = etl.extract_device_data(psql_engine)
analytics = etl.get_device_analytics(data)
etl.insert_analytics_data(analytics, analytics_table, mysql_engine)

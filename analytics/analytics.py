from dagster import job

from ops import (
    wait_for_data,
    extract_device_data,
    get_device_analytics,
    create_analytics_table,
    insert_analytics_data,
)
from db import get_psql_engine, get_mysql_engine


@job(resource_defs={"psql_engine": get_psql_engine, "mysql_engine": get_mysql_engine})
def device_analytics_etl():
    _ = wait_for_data()

    data = extract_device_data(_)
    analytics = get_device_analytics(data)
    analytics_table = create_analytics_table()
    insert_analytics_data(analytics, analytics_table)

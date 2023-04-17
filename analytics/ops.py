from time import sleep
from datetime import datetime

import pandas as pd
from dagster import op, Field
from psycopg2.errors import UndefinedTable
from sqlalchemy import (
    Column,
    Table,
    MetaData,
    DateTime,
    String,
    Integer,
    Float,
    text,
)
from utils import calc_end_to_end_distance


@op(
    config_schema={"min_rows": Field(int, default_value=50)},
    required_resource_keys={"psql_engine"},
)
def wait_for_data(context):
    context.log.info("Waiting for device data...")

    min_rows = context.op_config["min_rows"]
    psql_engine = context.resources.psql_engine
    while True:
        try:
            with psql_engine.connect() as conn:
                db_count = conn.execute(text("SELECT COUNT(*) FROM devices")).one()[0]
                context.log.info(f"{db_count=}")
                if db_count > min_rows:
                    return True
        except UndefinedTable:
            pass

        sleep(3)


@op(required_resource_keys={"psql_engine"})
def extract_device_data(context, _) -> pd.DataFrame:
    query = text(
        """
    SELECT
        device_id,
        temperature,
        location,
        time
    FROM devices
    """
    )

    psql_engine = context.resources.psql_engine
    with psql_engine.connect() as conn:
        results = conn.execute(query)
        df = pd.DataFrame(results.all(), columns=results.keys()).sort_values("time")

    context.log.info(f"Device data received: {df.shape}")

    return df


@op
def get_device_analytics(context, df: pd.DataFrame) -> pd.DataFrame:
    # convert timestamp into datetime, and round down the hour
    df["hour"] = (
        df["time"].astype(int).apply(datetime.fromtimestamp).dt.floor("H").apply(str)
    )

    # convert location string to a dict object
    df["location"] = df["location"].apply(eval)

    # will be summed to get count of data points
    df["n"] = 1

    aggs = {"temperature": "max", "n": "sum", "location": calc_end_to_end_distance}
    aggs_df = df.groupby(["device_id", "hour"]).agg(aggs).reset_index()
    aggs_df.rename(
        columns={
            "temperature": "max_temperature",
            "n": "data_points",
            "location": "total_distance",
        },
        inplace=True,
    )
    context.log.debug(aggs_df)

    return aggs_df


@op(required_resource_keys={"mysql_engine"})
def insert_analytics_data(context, data: pd.DataFrame, table: Table):
    mysql_engine = context.resources.mysql_engine
    with mysql_engine.connect() as conn:
        conn.execute(table.insert(), data.to_dict("records"))
        # conn.commit()
    context.log.info(f"Inserted {len(data)} records")


@op(required_resource_keys={"mysql_engine"})
def create_analytics_table(context) -> Table:
    mysql_engine = context.resources.mysql_engine
    metadata_obj = MetaData()
    analytics = Table(
        "analytics",
        metadata_obj,
        Column("device_id", String(255)),
        Column("hour", DateTime),
        Column("max_temperature", Integer),
        Column("total_distance", Float),
        Column("data_points", Integer),
    )
    metadata_obj.create_all(mysql_engine)

    return analytics

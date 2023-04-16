from datetime import datetime

import pandas as pd
from loguru import logger
from sqlalchemy import Engine, Table, text

from utils import calc_end_to_end_distance


def extract_device_data(psql_engine: Engine) -> pd.DataFrame:
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

    with psql_engine.connect() as conn:
        results = conn.execute(query)
        df = pd.DataFrame(results.all(), columns=results.keys()).sort_values("time")

    logger.success(f"Device data received: {df.shape}")

    return df


def get_device_analytics(df: pd.DataFrame) -> pd.DataFrame:
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
    logger.debug(aggs_df)

    return aggs_df


def insert_analytics_data(data: pd.DataFrame, table: Table, db_engine: Engine):
    with db_engine.connect() as conn:
        conn.execute(table.insert(), data.to_dict("records"))
        conn.commit()
    logger.success(f"Inserted {len(data)} records")

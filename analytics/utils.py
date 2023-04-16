from time import sleep

import pandas as pd
from loguru import logger
from geopy import distance
from sqlalchemy import Engine, text
from psycopg2.errors import UndefinedTable


def wait_for_data(psql_engine: Engine, min_rows: int = 50):
    logger.info("Waiting for device data...")
    while True:
        try:
            with psql_engine.connect() as conn:
                db_count = conn.execute(text("SELECT COUNT(*) FROM devices")).one()[0]
                logger.info(f"{db_count=}")
                if db_count > min_rows:
                    return
        except UndefinedTable:
            pass

        sleep(3)


def calc_end_to_end_distance(coordinates: pd.Series) -> float:
    coords_start = list(coordinates.iloc[0].values())
    coords_end = list(coordinates.iloc[-1].values())

    return distance.distance(coords_start, coords_end).km.real

from os import environ
from time import sleep

from loguru import logger
from sqlalchemy.exc import OperationalError
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    String,
    MetaData,
    Engine,
    Float,
    DateTime,
)


def get_psql_engine():
    return get_db_engine(environ["POSTGRESQL_CS"])


def get_mysql_engine():
    return get_db_engine(environ["MYSQL_CS"])


def get_db_engine(url: str) -> Engine:
    db_engine = create_engine(url, pool_pre_ping=True, pool_size=10)
    logger.success(db_engine)

    while True:
        try:
            conn = db_engine.connect()
            conn.close()
            logger.success("Connection is active.")
            break
        except OperationalError as e:
            logger.debug(e)
            sleep(1)

    return db_engine


def get_analytics_table(db_engine: Engine) -> Table:
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
    metadata_obj.create_all(db_engine)

    return analytics

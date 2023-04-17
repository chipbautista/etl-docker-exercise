from os import environ
from time import sleep

from dagster import resource
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine, engine


@resource
def get_psql_engine():
    return get_db_engine(environ["POSTGRESQL_CS"])


@resource
def get_mysql_engine():
    return get_db_engine(environ["MYSQL_CS"])


def get_db_engine(url: str) -> engine.Engine:
    db_engine = create_engine(url, pool_pre_ping=True, pool_size=10)
    print(db_engine)

    while True:
        try:
            conn = db_engine.connect()
            conn.close()
            print("Connection is active.")
            break
        except OperationalError as e:
            print(e)
            sleep(1)

    return db_engine

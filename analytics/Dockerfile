FROM python:3.8-slim
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2 sqlalchemy pandas geopy pymysql cryptography loguru
WORKDIR /app
COPY . .
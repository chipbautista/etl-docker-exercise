FROM python:3.8-slim
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install dagster dagit psycopg2 pandas geopy pymysql cryptography 
WORKDIR /app
COPY . .
FROM python:3.11-slim

WORKDIR /app

COPY ./dagster_loc/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY ./dagster_loc /app/dagster_loc
COPY ./data-generator /app/data-generator
COPY ./dbt_project /app/dbt_project

RUN mkdir -p /app/data-generator/nike_data 
RUN mkdir -p /data

ENV MINIO_USER=${MINIO_USER}
ENV MINIO_PASSWORD=${MINIO_PASSWORD}
ENV MINIO_ENDPOINT=http://minio:9000
ENV MINIO_BUCKET=nike-data
ENV DB_PATH=/data/nike_warehouse.duckdb 

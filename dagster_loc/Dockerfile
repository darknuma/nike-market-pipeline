FROM python:3.11-slim

# Set working directory
WORKDIR /app

COPY ./dagster_loc/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code
COPY ./dagster_loc /app/dagster_loc
COPY ./data-generator /app/data-generator
COPY ./dbt_project /app/dbt_project

# Ensure data directory exists (for shared volume)
RUN mkdir -p /app/data-generator/nike_data 
RUN mkdir -p /data

# Set environment variables for MinIO credentials and endpoint
# These are picked up by the resources in your Python code
ENV MINIO_USER=${MINIO_USER}
ENV MINIO_PASSWORD=${MINIO_PASSWORD}
ENV MINIO_ENDPOINT=minio:9000 
ENV MINIO_BUCKET=nike-data
ENV DB_PATH=/data/nike_warehouse.duckdb 

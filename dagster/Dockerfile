FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements file and install dependencies
COPY ./dagster/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Dagster code and data-generator code
COPY ./dagster /app/dagster
COPY ./data-generator /app/data-generator
COPY ./dbt_project /app/dbt_project 
#Copy dbt project as well

# Ensure data directory exists (though the op will create it)
RUN mkdir -p /app/data-generator/nike_data
RUN mkdir -p /data 
#Create directory for DuckDB data volume

ENV PYTHONPATH=/app


# Set environment variables for MinIO credentials and endpoint
ENV MINIO_USER=${MINIO_USER}
ENV MINIO_PASSWORD=${MINIO_PASSWORD}
ENV MINIO_ENDPOINT=minio:9000 
#Use service name for Docker internal communication
ENV MINIO_BUCKET=nike-data
ENV DB_PATH=/data/nike_warehouse.duckdb 
#Path for DuckDB within the container

# Command is set in docker-compose.yaml: dagster dev -h 0.0.0.0 -p 3000

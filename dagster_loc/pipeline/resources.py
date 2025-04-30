import os
import boto3
from botocore.config import Config
import duckdb
from dagster_loc import resource, ConfigurableResource
from typing import Any
from dagster_dbt import dbt_cli_resource, dbt_run_op


@resource
def minio_resource(context):
    """Resource for creating a MinIO client."""
    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
    minio_user = os.getenv('MINIO_USER')
    minio_password = os.getenv('MINIO_PASSWORD')

    if not minio_user or not minio_password:
        raise Exception("MINIO_USER and MINIO_PASSWORD environment variables must be set.")

    client = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_user,
        aws_secret_access_key=minio_password,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1' # MinIO often doesn't strictly use regions, but boto3 might expect one
    )

    # Optional: Ensure bucket exists on resource initialization
    bucket_name = os.getenv('MINIO_BUCKET', 'nike-data')
    try:
        client.head_bucket(Bucket=bucket_name)
        context.log.info(f"MinIO bucket '{bucket_name}' already exists.")
    except client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            context.log.info(f"MinIO bucket '{bucket_name}' not found, creating...")
            client.create_bucket(Bucket=bucket_name)
            context.log.info(f"Created MinIO bucket: '{bucket_name}'")
        else:
            raise # Re-raise other errors

    return client

@resource
def duckdb_resource(context):
    """Resource for creating a DuckDB connection."""
    db_path = os.getenv('DB_PATH', '/data/nike_warehouse.duckdb') # Matches docker-compose volume
    conn = duckdb.connect(database=db_path, read_only=False)
    return conn

# Resource for general configuration like paths
class DataGenerationConfig(ConfigurableResource):
    output_dir: str = "nike_data" # Local path within the container
    metadata_file: str = "nike_data/metadata.json" # Local path within the container
# @resource
# def gen_client(datagen: DataGenerationConfig):
#     """
#     Resource that provides a MinIO client for object storage operations.
#     """
#     return True 


# Configure the dbt CLI resource to point to your project and profile
@resource(config_schema={"project_dir": str, "profiles_dir": str})
def my_dbt_cli_resource(context):
    return dbt_cli_resource.configured(
        {
            "project_dir": context.resource_config["project_dir"],
            "profiles_dir": context.resource_config["profiles_dir"],
        }
    )(context)
import os
from pathlib import Path
import boto3
from botocore.config import Config
import duckdb
import pandas as pd
from dagster import (
    asset,
    job,
    op,
    resource,
    schedule,
    ConfigurableResource,
    AssetExecutionContext,
    Definitions
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

# ============= RESOURCES =============

@resource
def minio_resource(context):
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
        region_name='us-east-1'
    )

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
            raise

    return client

@resource
def duckdb_resource(context):
    db_path = os.getenv('DB_PATH', '/data/nike_warehouse.duckdb')
    conn = duckdb.connect(database=db_path, read_only=False)
    return conn

class DataGenerationConfig(ConfigurableResource):
    output_dir: str = "nike_data"
    metadata_file: str = "nike_data/metadata.json"

# ============= DBT ASSETS (Modern API) =============

DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent / "dbt_project"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
dbt_resource = DbtCliResource(project_dir=DBT_PROJECT_DIR)

@dbt_assets(manifest=dbt_project.manifest_path)
def nike_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# ============= RAW DATA ASSETS =============

@asset
def raw_sales_data(context: AssetExecutionContext) -> pd.DataFrame:
    context.log.info("Loading raw sales data")
    return pd.DataFrame()

@asset
def raw_inventory_data(context: AssetExecutionContext) -> pd.DataFrame:
    context.log.info("Loading raw inventory data")
    return pd.DataFrame()

# ============= OPS =============

@op
def generate_nike_data_op(context):
    context.log.info("Generating Nike data")
    return {"files": ["sales.csv", "inventory.csv"]}

@op
def upload_raw_data_to_minio_op(context, generated_files):
    context.log.info(f"Uploading files to MinIO: {generated_files}")
    return {"uploaded_files": generated_files["files"]}

@op
def load_raw_data_to_duckdb_op(context, uploaded_files_info):
    context.log.info(f"Loading files from MinIO to DuckDB: {uploaded_files_info}")
    return True

@op
def run_dbt_models_op(context, start_after):
    context.log.info("Running dbt models")
    return True

# ============= JOB =============

@job(
    resource_defs={
        "minio_resource": minio_resource,
        "duckdb_resource": duckdb_resource,
        "data_gen_config": DataGenerationConfig(
            output_dir="/app/data-generator/nike_data",
            metadata_file="/app/data-generator/nike_data/metadata.json"
        ),
        "dbt": dbt_resource
    }
)
def nike_data_pipeline():
    generated_files = generate_nike_data_op()
    uploaded_files_info = upload_raw_data_to_minio_op(generated_files=generated_files)
    raw_load_complete = load_raw_data_to_duckdb_op(uploaded_files_info=uploaded_files_info)
    run_dbt_models_op(start_after=raw_load_complete)

# ============= SCHEDULE =============

@schedule(cron_schedule="0 * * * *", job=nike_data_pipeline, execution_timezone="UTC")
def hourly_nike_data_schedule(context):
    return {
        "ops": {
            "generate_nike_data_op": {
                "config": {
                    "batch_size": 500
                }
            }
        }
    }

# ============= DEFINITIONS =============

defs = Definitions(
    assets=[
        raw_sales_data,
        raw_inventory_data,
        nike_dbt_assets,
    ],
    jobs=[nike_data_pipeline],
    schedules=[hourly_nike_data_schedule],
    resources={
        "minio_resource": minio_resource,
        "duckdb_resource": duckdb_resource,
        "dbt": dbt_resource,
        "data_gen_config": DataGenerationConfig(
            output_dir="/app/data-generator/nike_data",
            metadata_file="/app/data-generator/nike_data/metadata.json"
        )
    }
)

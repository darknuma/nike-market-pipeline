import os
import io
from pathlib import Path
import boto3
from botocore.config import Config as BotoConfig 
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
    Definitions,
    Config,
    OpExecutionContext
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
from .create_data import generate_nike_data

# ============= RESOURCES =============

@resource
def minio_resource(context):
    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')

    if not minio_endpoint.startswith('http://') and not minio_endpoint.startswith('https://'):
         minio_endpoint = f'http://{minio_endpoint}'

    minio_user = os.getenv('MINIO_USER')
    minio_password = os.getenv('MINIO_PASSWORD')

    if not minio_user or not minio_password:
        context.log.error("MINIO_USER and MINIO_PASSWORD environment variables must be set.")
        raise Exception("MinIO credentials not set.")

    client = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_user,
        aws_secret_access_key=minio_password,
        config=BotoConfig(signature_version='s3v4'),
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

# Define configuration classes for ops
class GenerateUploadConfig(Config):
    batch_size: int = 1000

# ============= DBT ASSETS (Modern API) =============

DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent / "dbt_project"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
dbt_resource = DbtCliResource(project_dir=DBT_PROJECT_DIR)

@dbt_assets(manifest=dbt_project.manifest_path)
def nike_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# ============= RAW DATA ASSETS =============

@asset
def raw_ad_events(context: AssetExecutionContext) -> None:
    """Load raw ad events data into DuckDB."""
    conn = duckdb.connect(database='/data/nike_warehouse.duckdb')
    conn.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE TABLE IF NOT EXISTS raw.ad_events (
            event_id VARCHAR,
            user_id VARCHAR,
            campaign_id VARCHAR,
            event_type VARCHAR,
            timestamp TIMESTAMP,
            platform VARCHAR,
            _loaded_at TIMESTAMP
        );
    """)
    conn.close()

@asset
def raw_campaigns(context: AssetExecutionContext) -> None:
    """Load raw campaigns data into DuckDB."""
    conn = duckdb.connect(database='/data/nike_warehouse.duckdb')
    conn.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE TABLE IF NOT EXISTS raw.campaigns (
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            start_date DATE,
            end_date DATE,
            budget DECIMAL,
            _loaded_at TIMESTAMP
        );
    """)
    conn.close()

@asset
def raw_conversions(context: AssetExecutionContext) -> None:
    """Load raw conversions data into DuckDB."""
    conn = duckdb.connect(database='/data/nike_warehouse.duckdb')
    conn.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE TABLE IF NOT EXISTS raw.conversions (
            conversion_id VARCHAR,
            user_id VARCHAR,
            campaign_id VARCHAR,
            product_id VARCHAR,
            timestamp TIMESTAMP,
            revenue DECIMAL,
            _loaded_at TIMESTAMP
        );
    """)
    conn.close()

@asset
def raw_products(context: AssetExecutionContext) -> None:
    """Load raw products data into DuckDB."""
    conn = duckdb.connect(database='/data/nike_warehouse.duckdb')
    conn.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE TABLE IF NOT EXISTS raw.products (
            product_id VARCHAR,
            product_name VARCHAR,
            category VARCHAR,
            price DECIMAL,
            _loaded_at TIMESTAMP
        );
    """)
    conn.close()

@asset
def raw_users(context: AssetExecutionContext) -> None:
    """Load raw users data into DuckDB."""
    conn = duckdb.connect(database='/data/nike_warehouse.duckdb')
    conn.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE TABLE IF NOT EXISTS raw.users (
            user_id VARCHAR,
            email VARCHAR,
            country VARCHAR,
            created_at TIMESTAMP,
            _loaded_at TIMESTAMP
        );
    """)
    conn.close() 

# ============= OPS =============

@op(
    required_resource_keys={"minio_resource"},
)
def generate_and_upload_nike_data_op(context):
    """Generate Nike data and upload directly to MinIO"""
    context.log.info("Generating Nike data and uploading to MinIO...")
    
    batch_size = 1000

    context.log.info(f"Using batch size: {batch_size}")
    
    result = generate_nike_data(batch_size=batch_size)
    context.log.info(f"Generated and uploaded data to MinIO: {result}")
    
    uploaded_keys = result["uploaded_keys"]
    return {"uploaded_keys": uploaded_keys}

@op(required_resource_keys={"minio_resource", "duckdb_resource"})
def load_raw_data_to_duckdb_op(context, data_keys):
    """Load data from MinIO into DuckDB"""
    minio_client = context.resources.minio_resource
    duckdb_conn = context.resources.duckdb_resource
    bucket_name = os.getenv('MINIO_BUCKET', 'nike-data')
    
    uploaded_keys = data_keys["uploaded_keys"]
    context.log.info(f"Processing {len(uploaded_keys)} keys to load into DuckDB")

    for key in uploaded_keys:
        if key.startswith("metadata/"):
            continue
            
        context.log.info(f"Loading {key} into DuckDB")
        
        try:
            # Stream file from MinIO
            obj = minio_client.get_object(Bucket=bucket_name, Key=key)
            parquet_bytes = obj['Body'].read()
            
            dataset_name = key.split('/')[0]
            context.log.info(f"Loading data for dataset: {dataset_name}")
            
            # Load into DuckDB from in-memory bytes
            df = pd.read_parquet(io.BytesIO(parquet_bytes))
            
            # Add _loaded_at timestamp
            df['_loaded_at'] = pd.Timestamp.now()
            
            duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
            

            if dataset_name == "users":
                duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS raw.users (
                        user_id VARCHAR,
                        name VARCHAR,
                        email VARCHAR,
                        segment VARCHAR,
                        signup_date DATE,
                        _loaded_at TIMESTAMP
                    )
                """)
            elif dataset_name == "products":
                duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS raw.products (
                        product_id VARCHAR,
                        name VARCHAR,
                        category VARCHAR,
                        price DOUBLE,
                        _loaded_at TIMESTAMP
                    )
                """)
            elif dataset_name == "campaigns":
                duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS raw.campaigns (
                        campaign_id VARCHAR,
                        name VARCHAR,
                        channel VARCHAR,
                        start_date DATE,
                        end_date DATE,
                        _loaded_at TIMESTAMP
                    )
                """)
            elif dataset_name == "ad_events":
                duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS raw.ad_events (
                        event_id VARCHAR,
                        user_id VARCHAR,
                        campaign_id VARCHAR,
                        event_type VARCHAR,
                        timestamp TIMESTAMP,
                        platform VARCHAR,
                        _loaded_at TIMESTAMP
                    )
                """)
            elif dataset_name == "conversions":
                duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS raw.conversions (
                        conversion_id VARCHAR,
                        user_id VARCHAR,
                        product_id VARCHAR,
                        campaign_id VARCHAR,
                        timestamp TIMESTAMP,
                        revenue DOUBLE,
                        _loaded_at TIMESTAMP
                    )
                """)
            
            duckdb_conn.execute(f"INSERT INTO raw.{dataset_name} SELECT * FROM df")
            context.log.info(f"Successfully loaded {len(df)} rows into raw.{dataset_name}")
            
        except Exception as e:
            context.log.error(f"Error loading {key} into DuckDB: {str(e)}")
            raise
    
    return True

@op(required_resource_keys={"dbt"}) # Only requires dbt resource
def run_dbt_models_op(context: OpExecutionContext, start_after): # Input name should match upstream output
    """Runs dbt build command."""
    context.log.info(f"Received start signal: {start_after}. Running dbt models...")

    dbt_cli_resource = context.resources.dbt

    try:
        dbt_cli_invocation = dbt_cli_resource.cli(["build"], context=context)
        for log_line in dbt_cli_invocation.stream():
            context.log.info(log_line) # Stream dbt logs to Dagster logs

        # Check for success
        dbt_cli_invocation.wait() # Wait for completion
        context.log.info("dbt build completed successfully.")
        # You could potentially inspect dbt_cli_invocation.get_artifact("run_results.json")
        # for more detailed results if needed.

    except Exception as e:
        context.log.error("dbt build command failed.")
        context.log.exception(e)
        raise 

    return True 

# ============= JOB =============

@job(
    resource_defs={
        "minio_resource": minio_resource,
        "duckdb_resource": duckdb_resource,
        "dbt": dbt_resource
    }
)
def nike_data_pipeline():
    """Main ETL pipeline for Nike data processing"""
    data_keys = generate_and_upload_nike_data_op()
    raw_load_complete = load_raw_data_to_duckdb_op(data_keys=data_keys)
    run_dbt_models_op(start_after=raw_load_complete)

# ============= SCHEDULE =============

@schedule(cron_schedule="0 * * * *", job=nike_data_pipeline, execution_timezone="UTC")
def hourly_nike_data_schedule(context):
    """
    Schedule for hourly execution of the Nike data pipeline.
    This returns the correct op config structure that Dagster expects.
    """
    return {
        "ops": {
            "generate_and_upload_nike_data_op": {
                 "config": {
                     # this batch size can be adjusted 
                "batch_size": 2000
            }
        }
    }
    }

# ============= DEFINITIONS =============

defs = Definitions(
    assets=[
        raw_ad_events,
        raw_campaigns,
        raw_conversions,
        raw_products,
        raw_users,
        nike_dbt_assets,
    ],
    jobs=[nike_data_pipeline],
    schedules=[hourly_nike_data_schedule],
    resources={
        "minio_resource": minio_resource,
        "duckdb_resource": duckdb_resource,
        "dbt": dbt_resource,
    }
)
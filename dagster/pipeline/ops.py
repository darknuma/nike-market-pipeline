import os
import uuid
import random
import json
import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker
import boto3
from botocore.exceptions import ClientError
import duckdb
from dagster import op, get_dagster_logger, In, Out, Nothing
from typing import List, Dict, Tuple

# Configuration (can be moved to resources or op configs if needed)
NIKE_SEGMENTS = ["Sneakerheads", "Runners", "Athletes", "Parents"]
NIKE_CHANNELS = ["Email", "Search", "Social", "App", "TikTok", "Instagram"]
NIKE_CATEGORIES = ["Running", "Basketball", "Jordan", "Training", "Lifestyle"]
NIKE_CAMPAIGNS = [
    "Air Max Day", "Back to School", "Jordan Heat Drop",
    "Run Club Boost", "Black Friday Blitz"
]
NIKE_PLATFORMS = ["iOS", "Android", "Web"]

# Initialize Faker (can be initialized within the op or resource if needed)
fake = Faker()
logger = get_dagster_logger()

# Helper Functions (can be kept here or moved to a separate utils file)
def load_last_timestamp(metadata_file):
    if os.path.exists(metadata_file):
        with open(metadata_file, "r") as f:
            timestamp_str = json.load(f).get("last_timestamp", None)
            return datetime.datetime.fromisoformat(timestamp_str) if timestamp_str else None
    return None

def save_last_timestamp(timestamp, metadata_file):
    os.makedirs(os.path.dirname(metadata_file), exist_ok=True)
    with open(metadata_file, "w") as f:
        json.dump({"last_timestamp": timestamp.isoformat()}, f)

def random_user():
    return {
        "user_id": str(uuid.uuid4()),
        "name": fake.name(),
        "email": fake.email(),
        "segment": random.choice(NIKE_SEGMENTS),
        "signup_date": fake.date_this_decade()
    }

def random_product():
    category = random.choice(NIKE_CATEGORIES)
    return {
        "product_id": str(uuid.uuid4()),
        "name": f"Nike {category} {fake.word().title()}",
        "category": category,
        "price": round(random.uniform(60, 250), 2)
    }

def random_campaign():
    return {
        "campaign_id": str(uuid.uuid4()),
        "name": random.choice(NIKE_CAMPAIGNS),
        "channel": random.choice(NIKE_CHANNELS),
        "start_date": fake.date_between(start_date="-2y", end_date="-1y"),
        "end_date": fake.date_between(start_date="-1y", end_date="today")
    }

def random_ad_interaction(user_id, campaign_id, last_timestamp):
    ts = fake.date_time_this_year()
    if last_timestamp and ts <= last_timestamp:
        return None
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "campaign_id": campaign_id,
        "event_type": random.choice(["impression", "click"]),
        "timestamp": ts,
        "platform": random.choice(NIKE_PLATFORMS)
    }

def random_conversion(user_id, product_id, campaign_id, last_timestamp):
    ts = fake.date_time_this_year()
    if last_timestamp and ts <= last_timestamp:
        return None
    return {
        "conversion_id": str(uuid.uuid4()),
        "user_id": user_id,
        "product_id": product_id,
        "campaign_id": campaign_id,
        "timestamp": ts,
        "revenue": round(random.uniform(60, 300), 2)
    }

# PyArrow Schemas
SCHEMAS = {
    "users": pa.schema([
        ("user_id", pa.string()), ("name", pa.string()), ("email", pa.string()),
        ("segment", pa.string()), ("signup_date", pa.date32())
    ]),
    "products": pa.schema([
        ("product_id", pa.string()), ("name", pa.string()),
        ("category", pa.string()), ("price", pa.float64())
    ]),
    "campaigns": pa.schema([
        ("campaign_id", pa.string()), ("name", pa.string()), ("channel", pa.string()),
        ("start_date", pa.date32()), ("end_date", pa.date32())
    ]),
    "ad_events": pa.schema([
        ("event_id", pa.string()), ("user_id", pa.string()), ("campaign_id", pa.string()),
        ("event_type", pa.string()), ("timestamp", pa.timestamp("ms")), ("platform", pa.string())
    ]),
    "conversions": pa.schema([
        ("conversion_id", pa.string()), ("user_id", pa.string()), ("product_id", pa.string()),
        ("campaign_id", pa.string()), ("timestamp", pa.timestamp("ms")), ("revenue", pa.float64())
    ])
}

@op(
    config_schema={"batch_size": int},
    required_resource_keys={"data_gen_config"}
)
def generate_nike_data_op(context) -> List[str]:
    """Generates new Nike data incrementally and saves to local Parquet files."""
    batch_size = context.op_config["batch_size"]
    output_dir = context.resources.data_gen_config.output_dir
    metadata_file = context.resources.data_gen_config.metadata_file

    os.makedirs(output_dir, exist_ok=True)

    last_timestamp = load_last_timestamp(metadata_file)
    new_timestamp = datetime.datetime.now()

    logger.info(f"Generating new data from: {last_timestamp} to {new_timestamp}")

    users, products, campaigns, ad_events, conversions = [], [], [], [], []

    # Use existing users, products, and campaigns if available to link new events/conversions
    existing_users = []
    existing_products = []
    existing_campaigns = []

    user_parquet_path = os.path.join(output_dir, "users.parquet")
    product_parquet_path = os.path.join(output_dir, "products.parquet")
    campaign_parquet_path = os.path.join(output_dir, "campaigns.parquet")

    if os.path.exists(user_parquet_path):
         existing_users = pq.read_table(user_parquet_path).to_pylist()
    if os.path.exists(product_parquet_path):
         existing_products = pq.read_table(product_parquet_path).to_pylist()
    if os.path.exists(campaign_parquet_path):
         existing_campaigns = pq.read_table(campaign_parquet_path).to_pylist()

    all_users = existing_users + users
    all_products = existing_products + products
    all_campaigns = existing_campaigns + campaigns

    # Generate new data (some potentially linked to existing)
    for _ in range(batch_size):
        u = random_user()
        users.append(u) # Add new user
        p = random_product()
        products.append(p) # Add new product
        c = random_campaign()
        campaigns.append(c) # Add new campaign

        # Generate interactions and conversions, linking to *any* user, product, campaign (new or existing)
        target_user = random.choice(all_users + [u]) if all_users else u
        target_campaign = random.choice(all_campaigns + [c]) if all_campaigns else c

        for _ in range(random.randint(1, 5)):
            ad = random_ad_interaction(target_user["user_id"], target_campaign["campaign_id"], last_timestamp)
            if ad:
                ad_events.append(ad)

        if random.random() < 0.5:
             target_product = random.choice(all_products + [p]) if all_products else p
             conv = random_conversion(target_user["user_id"], target_product["product_id"], target_campaign["campaign_id"], last_timestamp)
             if conv:
                 conversions.append(conv)


    # Write data to *new* temporary Parquet files for this batch
    # This prevents race conditions if multiple Dagster runs were happening
    # and makes the upload step simpler (upload only new files)
    output_files = {}
    temp_output_dir = os.path.join(output_dir, "temp", new_timestamp.strftime('%Y%m%d_%H%M%S'))
    os.makedirs(temp_output_dir, exist_ok=True)


    if users:
        file_path = os.path.join(temp_output_dir, "users.parquet")
        table = pa.Table.from_pylist(users, schema=SCHEMAS["users"])
        pq.write_table(table, file_path)
        output_files["users"] = file_path

    if products:
        file_path = os.path.join(temp_output_dir, "products.parquet")
        table = pa.Table.from_pylist(products, schema=SCHEMAS["products"])
        pq.write_table(table, file_path)
        output_files["products"] = file_path

    if campaigns:
        file_path = os.path.join(temp_output_dir, "campaigns.parquet")
        table = pa.Table.from_pylist(campaigns, schema=SCHEMAS["campaigns"])
        pq.write_table(table, file_path)
        output_files["campaigns"] = file_path

    if ad_events:
        file_path = os.path.join(temp_output_dir, "ad_events.parquet")
        table = pa.Table.from_pylist(ad_events, schema=SCHEMAS["ad_events"])
        pq.write_table(table, file_path)
        output_files["ad_events"] = file_path

    if conversions:
        file_path = os.path.join(temp_output_dir, "conversions.parquet")
        table = pa.Table.from_pylist(conversions, schema=SCHEMAS["conversions"])
        pq.write_table(table, file_path)
        output_files["conversions"] = file_path


    # Save the new timestamp after successful generation
    save_last_timestamp(new_timestamp, metadata_file)

    logger.info(f"✅ Generated new data batch saved to {temp_output_dir} up to {new_timestamp}")

    # Return a list of generated file paths (absolute or relative to output_dir)
    return list(output_files.values())


@op(required_resource_keys={"minio_resource", "data_gen_config"})
def upload_raw_data_to_minio_op(context, generated_files: List[str]):
    """Uploads locally generated Parquet files to MinIO with timestamped paths."""
    minio_client = context.resources.minio_resource
    minio_bucket = os.getenv('MINIO_BUCKET', 'nike-data')
    raw_prefix = "raw/"
    output_dir = context.resources.data_gen_config.output_dir # Need this to get the temp subdir

    uploaded_files_info = []

    if not generated_files:
        logger.warning("No files generated in the previous step to upload.")
        return uploaded_files_info

    # The timestamp is part of the generated_files paths (e.g., nike_data/temp/YYYYMMDD_HHMMSS/file.parquet)
    # Extract the timestamp from the first file path
    # Assuming all files generated in a single batch share the same timestamp subdirectory
    timestamp_subdir = os.path.basename(os.path.dirname(generated_files[0]))


    for local_path in generated_files:
        filename = os.path.basename(local_path)
        table_name = filename.split('.')[0] # users, products, etc.
        # Construct the S3 key: raw/<table_name>/YYYYMMDD_HHMMSS.parquet
        s3_key = f"{raw_prefix}{table_name}/{timestamp_subdir}.parquet"

        try:
            minio_client.upload_file(local_path, minio_bucket, s3_key)
            logger.info(f"Uploaded {local_path} to s3://{minio_bucket}/{s3_key}")
            uploaded_files_info.append({"table": table_name, "key": s3_key})

            # Optional: Clean up the local temporary file after upload
            # os.remove(local_path)

        except ClientError as e:
            logger.error(f"Error uploading {local_path} to {s3_key}: {e}")
            # Depending on your error handling strategy, you might want to raise
            # or mark this op as failed.
            raise

    # Optional: Clean up the temporary timestamp directory locally after all files are uploaded
    # import shutil
    # temp_dir = os.path.dirname(generated_files[0])
    # if os.path.exists(temp_dir):
    #     shutil.rmtree(temp_dir)
    #     logger.info(f"Cleaned up local temporary directory: {temp_dir}")


    logger.info("✅ Data upload to MinIO completed.")
    return uploaded_files_info # Return info about what was uploaded

@op(required_resource_keys={"minio_resource", "duckdb_resource"})
def load_raw_data_to_duckdb_op(context, uploaded_files_info: List[Dict[str, str]]):
    """Loads the latest raw Parquet files from MinIO into DuckDB using MERGE for CDC."""
    minio_client = context.resources.minio_resource
    duckdb_conn = context.resources.duckdb_resource
    minio_bucket = os.getenv('MINIO_BUCKET', 'nike-data')
    raw_prefix = "raw/"

    # Table schemas for DuckDB (Add _loaded_at)
    TABLE_SCHEMAS_DUCKDB = {
        "users": """
            CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR,
                name VARCHAR,
                email VARCHAR,
                segment VARCHAR,
                signup_date DATE,
                _loaded_at TIMESTAMP,
                PRIMARY KEY (user_id)
            )
        """,
        "products": """
            CREATE TABLE IF NOT EXISTS products (
                product_id VARCHAR,
                name VARCHAR,
                category VARCHAR,
                price DOUBLE,
                _loaded_at TIMESTAMP,
                PRIMARY KEY (product_id)
            )
        """,
        "campaigns": """
            CREATE TABLE IF NOT EXISTS campaigns (
                campaign_id VARCHAR,
                name VARCHAR,
                channel VARCHAR,
                start_date DATE,
                end_date DATE,
                _loaded_at TIMESTAMP,
                PRIMARY KEY (campaign_id)
            )
        """,
        "ad_events": """
            CREATE TABLE IF NOT EXISTS ad_events (
                event_id VARCHAR,
                user_id VARCHAR,
                campaign_id VARCHAR,
                event_type VARCHAR,
                timestamp TIMESTAMP,
                platform VARCHAR,
                _loaded_at TIMESTAMP,
                PRIMARY KEY (event_id)
            )
        """,
        "conversions": """
            CREATE TABLE IF NOT EXISTS conversions (
                conversion_id VARCHAR,
                user_id VARCHAR,
                product_id VARCHAR,
                campaign_id VARCHAR,
                timestamp TIMESTAMP,
                revenue DOUBLE,
                _loaded_at TIMESTAMP,
                PRIMARY KEY (conversion_id)
            )
        """
    }

    # Ensure tables exist
    for table_name, schema in TABLE_SCHEMAS_DUCKDB.items():
         duckdb_conn.execute(schema)

    if not uploaded_files_info:
         logger.warning("No uploaded files information received. Skipping DuckDB load.")
         return

    # Process each uploaded file
    for file_info in uploaded_files_info:
        table_name = file_info['table']
        s3_key = file_info['key']

        logger.info(f"Processing s3://{minio_bucket}/{s3_key} for table {table_name}")

        # DuckDB can read directly from S3/MinIO with the httpfs extension
        # Need to configure S3 credentials for DuckDB
        # This requires the httpfs and motherduck extensions (motherduck includes httpfs)
        # Ensure these are installed in your Dagster Dockerfile and loaded in DuckDB
        # https://duckdb.org/docs/guides/import/s3_import

        try:
            duckdb_conn.execute("INSTALL httpfs;")
            duckdb_conn.execute("LOAD httpfs;")
            # Configure s3 credentials for DuckDB (using environment variables)
            duckdb_conn.execute(f"SET s3_endpoint='{os.getenv('MINIO_ENDPOINT', 'minio:9000').replace('http://', '')}';")
            duckdb_conn.execute("SET s3_access_key_id='" + os.getenv('MINIO_USER') + "';")
            duckdb_conn.execute("SET s3_secret_access_key='" + os.getenv('MINIO_PASSWORD') + "';")
            duckdb_conn.execute("SET s3_use_ssl=false;") # Assuming http, not https for MinIO

            # Read the parquet file directly into a temporary DuckDB table
            temp_table_name = f"temp_{table_name}"
            duckdb_conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            # Add the _loaded_at column during the read
            duckdb_conn.execute(f"""
                 CREATE TEMP TABLE {temp_table_name} AS
                 SELECT *, now() as _loaded_at
                 FROM read_parquet('s3://{minio_bucket}/{s3_key}');
             """)

            # Perform the MERGE operation
            # Assuming primary keys are user_id, product_id, campaign_id, event_id, conversion_id
            primary_key_col = f"{table_name.rstrip('s')}_id" if table_name in ["users", "products", "campaigns"] else f"{table_name.rstrip('s')}_id" # Handles conversions/events

            # Get columns dynamically from the temp table schema
            cols = [col[0] for col in duckdb_conn.execute(f"PRAGMA table_info('{temp_table_name}')").fetchall()]
            # Exclude the primary key from the UPDATE SET list
            update_cols = [col for col in cols if col != primary_key_col]

            merge_query = f"""
                 MERGE INTO {table_name} t
                 USING {temp_table_name} s
                 ON t.{primary_key_col} = s.{primary_key_col}
                 WHEN MATCHED THEN
                     UPDATE SET
                         {', '.join(f"{col} = s.{col}" for col in update_cols)}
                 WHEN NOT MATCHED THEN
                     INSERT ({', '.join(cols)})
                     VALUES ({', '.join(f"s.{col}" for col in cols)});
             """
            duckdb_conn.execute(merge_query)
            logger.info(f"Successfully merged data from {s3_key} into DuckDB table '{table_name}'")

            # Clean up temp table
            duckdb_conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")


        except Exception as e:
            logger.error(f"❌ Error loading data for table {table_name} from {s3_key}: {e}")
            # Handle errors - maybe log and continue, or re-raise to fail the op/job
            raise

    logger.info("✅ Data loading into DuckDB completed.")
    # Returning Nothing signifies that this op doesn't produce data for downstream ops directly
    # The output is the state change in the DuckDB database.
    return Nothing


# You will also need ops for running dbt models.
# The dagster-dbt library provides utilities for this.
# Assuming your dbt project is in ./dbt_project
from dagster_dbt import dbt_cli_resource, dbt_run_op

# Configure the dbt CLI resource to point to your project and profile
@resource(config_schema={"project_dir": str, "profiles_dir": str})
def my_dbt_cli_resource(context):
    return dbt_cli_resource.configured(
        {
            "project_dir": context.resource_config["project_dir"],
            "profiles_dir": context.resource_config["profiles_dir"],
        }
    )(context)


# Define an op to run dbt models (e.g., your silver layer)
@op(required_resource_keys={"dbt"})
def run_dbt_models_op(context):
    """Runs dbt models."""
    # You can target specific models, select by tag, etc.
    dbt_run = context.resources.dbt.run()
    # You can also run specific commands like `dbt test`, `dbt build`, etc.
    # context.resources.dbt.build()
    logger.info(f"dbt run completed with result: {dbt_run}")
    return Nothing # Or return results if needed downstream
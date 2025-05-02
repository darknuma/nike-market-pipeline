import os
import boto3
from botocore.config import Config
import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
import json

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000') 
RAW_PREFIX = "raw/"
DB_PATH = "/data/nike_warehouse.duckdb" 
MINIO_BUCKET="nike-data"

TABLE_SCHEMAS = {
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

def get_minio_client():
    """Create and return a MinIO client using boto3"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=os.getenv('MINIO_USER'),
        aws_secret_access_key=os.getenv('MINIO_PASSWORD'),
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def get_latest_parquet_files(client, table_name):
    """Get the latest Parquet file for each table from MinIO"""
    try:
        response = client.list_objects_v2(
            Bucket=MINIO_BUCKET,
            Prefix=f"{RAW_PREFIX}{table_name}/"
        )
        
        if 'Contents' not in response:
            return None
            
        latest_file = max(
            response['Contents'],
            key=lambda x: x['Key'].split('/')[-1].split('.')[0]
        )
        
        return latest_file['Key']
    except Exception as e:
        print(f"Error getting latest file for {table_name}: {e}")
        return None

def load_table_from_minio(client, conn, table_name):
    """Load data from MinIO into DuckDB with CDC support"""
    try:
        latest_file = get_latest_parquet_files(client, table_name)
        if not latest_file:
            print(f"No files found for table {table_name}")
            return
            
        temp_file = f"temp_{table_name}.parquet"
        client.download_file(MINIO_BUCKET, latest_file, temp_file)
        
        table = pq.read_table(temp_file)
        
        current_time = datetime.now()
        table = table.append_column('_loaded_at', 
                                  pa.array([current_time] * len(table)))
        
        temp_table_name = f"temp_{table_name}"
        conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
        conn.execute(f"CREATE TABLE {temp_table_name} AS SELECT * FROM table")
        
        merge_query = f"""
            MERGE INTO {table_name} t
            USING {temp_table_name} s
            ON t.{table_name[:-1]}_id = s.{table_name[:-1]}_id
            WHEN MATCHED THEN
                UPDATE SET
                    {', '.join(f"{col} = s.{col}" for col in table.column_names if col != f"{table_name[:-1]}_id")}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(table.column_names)})
                VALUES ({', '.join(f"s.{col}" for col in table.column_names)})
        """
        conn.execute(merge_query)
        
        conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
        os.remove(temp_file)
        
        print(f"✅ Successfully loaded data for {table_name}")
        
    except Exception as e:
        print(f"❌ Error loading table {table_name}: {e}")

def main():
    """Main function to handle the loading process"""
    try:
        minio_client = get_minio_client()
        
        conn = duckdb.connect(DB_PATH)
        
        for table_name, schema in TABLE_SCHEMAS.items():
            conn.execute(schema)
        
        for table_name in TABLE_SCHEMAS.keys():
            load_table_from_minio(minio_client, conn, table_name)
        
        print("✅ Data loading completed successfully")
        
    except Exception as e:
        print(f"❌ Error during loading process: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()

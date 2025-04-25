# so this part we are going to use boto3 with minio

import os
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import json
from datetime import datetime
from data-generator import create_data 

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_BUCKET = "nike-data"
DATA_DIR = "nike_data"

def get_minio_client():
    """Create and return a MinIO client using boto3"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=os.getenv('MINIO_USER'),
        aws_secret_access_key=os.getenv('MINIO_PASSWORD'),
        config=Config(signature_version='s3v4'),
        # region_name='us-east-1'  # MinIO doesn't care about region, but boto3 requires it
    )

def ensure_bucket_exists(client):
    """Ensure the MinIO bucket exists, create if it doesn't"""
    try:
        client.head_bucket(Bucket=MINIO_BUCKET)
    except ClientError:
        client.create_bucket(Bucket=MINIO_BUCKET)
        print(f"Created bucket: {MINIO_BUCKET}")

def upload_parquet_files(client):
    """Upload all Parquet files from the data directory to MinIO"""
    if not os.path.exists(DATA_DIR):
        print(f"Data directory {DATA_DIR} not found!")
        return

    # Get the last processed timestamp
    metadata_file = os.path.join(DATA_DIR, "metadata.json")
    last_processed = None
    if os.path.exists(metadata_file):
        with open(metadata_file, 'r') as f:
            last_processed = json.load(f).get('last_timestamp')

    # Upload each Parquet file
    for filename in os.listdir(DATA_DIR):
        if filename.endswith('.parquet'):
            local_path = os.path.join(DATA_DIR, filename)
            
            # Upload with timestamp in the path for versioning
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            s3_key = f"{filename.split('.')[0]}/{timestamp}.parquet"
            
            try:
                client.upload_file(local_path, MINIO_BUCKET, s3_key)
                print(f"Uploaded {filename} to {s3_key}")
            except ClientError as e:
                print(f"Error uploading {filename}: {e}")

def main():
    """Main function to handle the ingestion process"""
    try:
        client = get_minio_client()
        ensure_bucket_exists(client)
        upload_parquet_files(client)
        print("✅ Data ingestion completed successfully")
    except Exception as e:
        print(f"❌ Error during ingestion: {e}")

if __name__ == "__main__":
    main()
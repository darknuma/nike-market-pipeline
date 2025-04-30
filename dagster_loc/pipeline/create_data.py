# create_data.py

import os
import uuid
import random
import json
import datetime
import io
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker
import boto3
from botocore.config import Config

# Config
DEFAULT_BATCH_SIZE = 1000
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "nike-data")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

# Initialize Faker and S3 client
fake = Faker()
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=os.getenv("MINIO_USER"),
    aws_secret_access_key=os.getenv("MINIO_PASSWORD"),
    config=Config(signature_version="s3v4"),
)

# Schema mappings
SCHEMAS = {
    "users": [("user_id", pa.string()), ("name", pa.string()), ("email", pa.string()), ("segment", pa.string()), ("signup_date", pa.date32())],
    "products": [("product_id", pa.string()), ("name", pa.string()), ("category", pa.string()), ("price", pa.float64())],
    "campaigns": [("campaign_id", pa.string()), ("name", pa.string()), ("channel", pa.string()), ("start_date", pa.date32()), ("end_date", pa.date32())],
    "ad_events": [("event_id", pa.string()), ("user_id", pa.string()), ("campaign_id", pa.string()), ("event_type", pa.string()), ("timestamp", pa.timestamp("ms")), ("platform", pa.string())],
    "conversions": [("conversion_id", pa.string()), ("user_id", pa.string()), ("product_id", pa.string()), ("campaign_id", pa.string()), ("timestamp", pa.timestamp("ms")), ("revenue", pa.float64())],
}

# Nike-specific settings
NIKE_SEGMENTS = ["Sneakerheads", "Runners", "Athletes", "Parents"]
NIKE_CHANNELS = ["Email", "Search", "Social", "App", "TikTok", "Instagram"]
NIKE_CATEGORIES = ["Running", "Basketball", "Jordan", "Training", "Lifestyle"]
NIKE_CAMPAIGNS = ["Air Max Day", "Back to School", "Jordan Heat Drop", "Run Club Boost", "Black Friday Blitz"]
NIKE_PLATFORMS = ["iOS", "Android", "Web"]

def random_user():
    return {"user_id": str(uuid.uuid4()), "name": fake.name(), "email": fake.email(), "segment": random.choice(NIKE_SEGMENTS), "signup_date": fake.date_this_decade()}

def random_product():
    category = random.choice(NIKE_CATEGORIES)
    return {"product_id": str(uuid.uuid4()), "name": f"Nike {category} {fake.word().title()}", "category": category, "price": round(random.uniform(60, 250), 2)}

def random_campaign():
    return {"campaign_id": str(uuid.uuid4()), "name": random.choice(NIKE_CAMPAIGNS), "channel": random.choice(NIKE_CHANNELS), "start_date": fake.date_between(start_date="-2y", end_date="-1y"), "end_date": fake.date_between(start_date="-1y", end_date="today")}

def random_ad_interaction(user_id, campaign_id):
    return {"event_id": str(uuid.uuid4()), "user_id": user_id, "campaign_id": campaign_id, "event_type": random.choice(["impression", "click"]), "timestamp": fake.date_time_this_year(), "platform": random.choice(NIKE_PLATFORMS)}

def random_conversion(user_id, product_id, campaign_id):
    return {"conversion_id": str(uuid.uuid4()), "user_id": user_id, "product_id": product_id, "campaign_id": campaign_id, "timestamp": fake.date_time_this_year(), "revenue": round(random.uniform(60, 300), 2)}

def upload_data_to_minio(data: list, dataset_name: str):
    """Convert list to Parquet and upload directly to MinIO"""
    if not data:
        return None

    schema = pa.schema(SCHEMAS[dataset_name])
    table = pa.Table.from_pylist(data, schema=schema)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    s3_key = f"{dataset_name}/{timestamp}.parquet"

    s3_client.upload_fileobj(buffer, MINIO_BUCKET, s3_key)
    print(f"✅ Uploaded {dataset_name} → {s3_key}")
    return s3_key

def upload_metadata_to_minio(timestamp: datetime.datetime):
    """Upload metadata.json to MinIO"""
    metadata = {"last_timestamp": timestamp.isoformat()}
    buffer = io.BytesIO(json.dumps(metadata).encode("utf-8"))
    s3_key = f"metadata/{timestamp.strftime('%Y%m%d_%H%M%S')}.json"

    s3_client.upload_fileobj(buffer, MINIO_BUCKET, s3_key)
    print(f"✅ Uploaded metadata.json → {s3_key}")
    return s3_key

def generate_nike_data(batch_size=DEFAULT_BATCH_SIZE):
    """Generate and upload data directly to MinIO"""
    timestamp = datetime.datetime.now()
    users, products, campaigns, ad_events, conversions = [], [], [], [], []

    for _ in range(batch_size):
        user = random_user()
        product = random_product()
        campaign = random_campaign()
        users.append(user)
        products.append(product)
        campaigns.append(campaign)

        for _ in range(random.randint(1, 3)):
            ad = random_ad_interaction(user["user_id"], campaign["campaign_id"])
            ad_events.append(ad)

        if random.random() < 0.5:
            conv = random_conversion(user["user_id"], product["product_id"], campaign["campaign_id"])
            conversions.append(conv)

    uploaded_keys = []
    uploaded_keys.append(upload_data_to_minio(users, "users"))
    uploaded_keys.append(upload_data_to_minio(products, "products"))
    uploaded_keys.append(upload_data_to_minio(campaigns, "campaigns"))
    uploaded_keys.append(upload_data_to_minio(ad_events, "ad_events"))
    uploaded_keys.append(upload_data_to_minio(conversions, "conversions"))
    uploaded_keys.append(upload_metadata_to_minio(timestamp))

    return {"uploaded_keys": [k for k in uploaded_keys if k]}

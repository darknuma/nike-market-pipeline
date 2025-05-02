import os
import uuid
import random
import json
import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

# Configuration
DEFAULT_BATCH_SIZE = 1000  # Adjust batch size as needed
OUTPUT_DIR = "nike_data"
METADATA_FILE = f"{OUTPUT_DIR}/metadata.json"

NIKE_SEGMENTS = ["Sneakerheads", "Runners", "Athletes", "Parents"]
NIKE_CHANNELS = ["Email", "Search", "Social", "App", "TikTok", "Instagram"]
NIKE_CATEGORIES = ["Running", "Basketball", "Jordan", "Training", "Lifestyle"]
NIKE_CAMPAIGNS = [
    "Air Max Day", "Back to School", "Jordan Heat Drop",
    "Run Club Boost", "Black Friday Blitz"
]
NIKE_PLATFORMS = ["iOS", "Android", "Web"]

fake = Faker()


# ==========================  Helper Functions  ========================== #

def load_last_timestamp():
    """ Load the last processed timestamp from metadata.json """
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            return json.load(f).get("last_timestamp", None)
    return None


def save_last_timestamp(timestamp):
    """ Save the latest processed timestamp to metadata.json """
    with open(METADATA_FILE, "w") as f:
        json.dump({"last_timestamp": timestamp.isoformat()}, f)


def append_parquet(data, filename, schema):
    """ Append new data to an existing Parquet file """
    path = f"{OUTPUT_DIR}/{filename}"
    
    if os.path.exists(path):
        existing_table = pq.read_table(path)
        new_table = pa.Table.from_pylist(data, schema=pa.schema(schema))
        merged_table = pa.concat_tables([existing_table, new_table])
        pq.write_table(merged_table, path)
    else:
        table = pa.Table.from_pylist(data, schema=pa.schema(schema))
        pq.write_table(table, path)


# ==========================  Data Generators  ========================== #

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


# ==========================  Main Function  ========================== #

def generate_nike_data(batch_size=DEFAULT_BATCH_SIZE):
    """ Generates new Nike data in an incremental way """
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    last_timestamp = load_last_timestamp()
    new_timestamp = datetime.datetime.now()

    print(f"Generating new data from: {last_timestamp} to {new_timestamp}")

    users, products, campaigns, ad_events, conversions = [], [], [], [], []

    for _ in range(batch_size):
        u = random_user()
        users.append(u)
        p = random_product()
        products.append(p)
        c = random_campaign()
        campaigns.append(c)

        for _ in range(random.randint(1, 5)):
            ad = random_ad_interaction(u["user_id"], c["campaign_id"], last_timestamp)
            if ad:
                ad_events.append(ad)

        if random.random() < 0.5:
            conv = random_conversion(u["user_id"], p["product_id"], c["campaign_id"], last_timestamp)
            if conv:
                conversions.append(conv)

    append_parquet(users, "users.parquet", [
        ("user_id", pa.string()), ("name", pa.string()), ("email", pa.string()), 
        ("segment", pa.string()), ("signup_date", pa.date32())
    ])
    
    append_parquet(products, "products.parquet", [
        ("product_id", pa.string()), ("name", pa.string()), 
        ("category", pa.string()), ("price", pa.float64())
    ])
    
    append_parquet(campaigns, "campaigns.parquet", [
        ("campaign_id", pa.string()), ("name", pa.string()), ("channel", pa.string()), 
        ("start_date", pa.date32()), ("end_date", pa.date32())
    ])
    
    append_parquet(ad_events, "ad_events.parquet", [
        ("event_id", pa.string()), ("user_id", pa.string()), ("campaign_id", pa.string()), 
        ("event_type", pa.string()), ("timestamp", pa.timestamp("ms")), ("platform", pa.string())
    ])
    
    append_parquet(conversions, "conversions.parquet", [
        ("conversion_id", pa.string()), ("user_id", pa.string()), ("product_id", pa.string()), 
        ("campaign_id", pa.string()), ("timestamp", pa.timestamp("ms")), ("revenue", pa.float64())
    ])

    save_last_timestamp(new_timestamp)
    print(f"✅ New data generated and saved up to {new_timestamp}")


# ==========================  Run Script  ========================== #

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", type=int, default=DEFAULT_BATCH_SIZE, help="Number of rows per batch")
    args = parser.parse_args()

    generate_nike_data(args.batch_size)

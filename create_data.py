import os
import uuid
import random
import string
import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

fake = Faker()

# Configuration
DEFAULT_SIZE_GB = 1
OUTPUT_DIR = "nike_data"
BYTES_PER_ROW = 500  # rough estimate for row size

# Nike-specific settings
NIKE_SEGMENTS = ["Sneakerheads", "Runners", "Athletes", "Parents"]
NIKE_CHANNELS = ["Email", "Search", "Social", "App", "TikTok", "Instagram"]
NIKE_CATEGORIES = ["Running", "Basketball", "Jordan", "Training", "Lifestyle"]
NIKE_CAMPAIGNS = [
    "Air Max Day",
    "Back to School",
    "Jordan Heat Drop",
    "Run Club Boost",
    "Black Friday Blitz"
]
NIKE_PLATFORMS = ["iOS", "Android", "Web"]


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

def random_ad_interaction(user_id, campaign_id):
    ts = fake.date_time_this_year()
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "campaign_id": campaign_id,
        "event_type": random.choice(["impression", "click"]),
        "timestamp": ts,
        "platform": random.choice(NIKE_PLATFORMS)
    }

def random_conversion(user_id, product_id, campaign_id):
    ts = fake.date_time_this_year()
    return {
        "conversion_id": str(uuid.uuid4()),
        "user_id": user_id,
        "product_id": product_id,
        "campaign_id": campaign_id,
        "timestamp": ts,
        "revenue": round(random.uniform(60, 300), 2)
    }

def write_parquet(data, schema, path):
    table = pa.Table.from_pylist(data, schema=schema)
    pq.write_table(table, path)


def generate_nike_data(total_size_gb=DEFAULT_SIZE_GB):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    target_rows = int((total_size_gb * (1024**3)) / BYTES_PER_ROW)
    print(f"Generating approximately {target_rows:,} rows of data")

    users, products, campaigns = [], [], []
    ad_events, conversions = [], []

    for _ in range(target_rows // 100):
        u = random_user()
        users.append(u)
        p = random_product()
        products.append(p)
        c = random_campaign()
        campaigns.append(c)

        for _ in range(random.randint(1, 5)):
            ad = random_ad_interaction(u["user_id"], c["campaign_id"])
            ad_events.append(ad)

        if random.random() < 0.5:
            conversions.append(random_conversion(u["user_id"], p["product_id"], c["campaign_id"]))

    write_parquet(users, pa.schema([
        ("user_id", pa.string()),
        ("name", pa.string()),
        ("email", pa.string()),
        ("segment", pa.string()),
        ("signup_date", pa.date32())
    ]), f"{OUTPUT_DIR}/users.parquet")

    write_parquet(products, pa.schema([
        ("product_id", pa.string()),
        ("name", pa.string()),
        ("category", pa.string()),
        ("price", pa.float64())
    ]), f"{OUTPUT_DIR}/products.parquet")

    write_parquet(campaigns, pa.schema([
        ("campaign_id", pa.string()),
        ("name", pa.string()),
        ("channel", pa.string()),
        ("start_date", pa.date32()),
        ("end_date", pa.date32())
    ]), f"{OUTPUT_DIR}/campaigns.parquet")

    write_parquet(ad_events, pa.schema([
        ("event_id", pa.string()),
        ("user_id", pa.string()),
        ("campaign_id", pa.string()),
        ("event_type", pa.string()),
        ("timestamp", pa.timestamp("ms")),
        ("platform", pa.string())
    ]), f"{OUTPUT_DIR}/ad_events.parquet")

    write_parquet(conversions, pa.schema([
        ("conversion_id", pa.string()),
        ("user_id", pa.string()),
        ("product_id", pa.string()),
        ("campaign_id", pa.string()),
        ("timestamp", pa.timestamp("ms")),
        ("revenue", pa.float64())
    ]), f"{OUTPUT_DIR}/conversions.parquet")

    print(f"Data generation complete. Files saved to {OUTPUT_DIR}/")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--size_gb", type=int, default=1, help="Size of data to generate in GB")
    args = parser.parse_args()

    generate_nike_data(args.size_gb)

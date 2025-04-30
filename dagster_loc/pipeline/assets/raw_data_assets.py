from dagster import asset, AssetExecutionContext
import pandas as pd
import duckdb

@asset
def raw_ad_events(context: AssetExecutionContext) -> None:
    """Load raw ad events data into DuckDB."""
    conn = duckdb.connect(database='/data/nike_market.duckdb')
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
    conn = duckdb.connect(database='/data/nike_market.duckdb')
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
    conn = duckdb.connect(database='/data/nike_market.duckdb')
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
    conn = duckdb.connect(database='/data/nike_market.duckdb')
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
    conn = duckdb.connect(database='/data/nike_market.duckdb')
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
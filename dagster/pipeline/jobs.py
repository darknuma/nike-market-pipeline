from dagster import job, op, Out, In, Nothing
import os
import sys

# Add the generator directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from generator.load import main as load_data

@op
def load_from_minio_to_duckdb():
    """Load data from MinIO to DuckDB"""
    load_data()
    return Nothing

@job
def load_data_job():
    """Job to load data from MinIO to DuckDB"""
    load_from_minio_to_duckdb() 
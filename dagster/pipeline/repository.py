from dagster import repository
from .jobs import load_data_job

@repository
def nike_market_repository():
    """Repository for Nike Market Pipeline"""
    return [load_data_job] 
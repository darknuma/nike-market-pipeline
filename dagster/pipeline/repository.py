from dagster import load_assets_from_modules, repository

from . import assets
from .jobs import nike_data_pipeline # Import your job

@repository
def nike_data_repo():
    """
    This repository contains the Nike data pipeline.
    """
    return [
        nike_data_pipeline,
        *load_assets_from_modules([assets]) # Load assets defined in assets.py
    ]
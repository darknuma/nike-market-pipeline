from dagster import asset, AssetExecutionContext
from typing import List, Dict
import pandas as pd

@asset
def raw_sales_data(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Asset that represents the raw sales data from the source.
    """
    # TODO: Implement the actual data loading logic
    context.log.info("Loading raw sales data")
    return pd.DataFrame()

@asset
def raw_inventory_data(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Asset that represents the raw inventory data from the source.
    """
    # TODO: Implement the actual data loading logic
    context.log.info("Loading raw inventory data")
    return pd.DataFrame() 
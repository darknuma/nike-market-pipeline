from dagster import AssetsDefinition
from dagster_dbt import load_assets_from_dbt_project
import os

# Define the local path to your dbt project relative to where Dagster is running
DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), "..", "dbt_project")
DBT_PROFILES_DIR = os.path.join(DBT_PROJECT_DIR, ".") # Point to the project dir itself if profiles.yml is there

# Load dbt models as Dagster assets
dbt_assets: AssetsDefinition = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
    # Optional: Specify an I/O manager for writing dbt outputs if needed
    # io_manager_key="...",
    # Optional: Add tags or other metadata
    # key_prefix="silver" # Prefix assets with 'silver' if they represent the silver layer
)

# You could also define Assets representing the raw data in MinIO
# This would involve writing custom I/O managers or using libraries like dagster-aws (if using S3 directly)
# For simplicity in this example, we'll focus on the dbt assets and the ops that produce the raw data.
# ============= ASSETS =============

# Define the local path to your dbt project relative to where Dagster is running
DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), "..", "dbt_project")
DBT_PROFILES_DIR = os.path.join(DBT_PROJECT_DIR, ".") # Point to the project dir itself if profiles.yml is there

# Load dbt models as Dagster assets
try:
    dbt_assets: AssetsDefinition = load_assets_from_dbt_project(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        # Optional: Specify an I/O manager for writing dbt outputs if needed
        # io_manager_key="...",
        # Optional: Add tags or other metadata
        # key_prefix="silver" # Prefix assets with 'silver' if they represent the silver layer
    )
except Exception as e:
    print(f"Warning: Could not load dbt assets: {e}")
    print("This is normal if dbt hasn't been compiled yet.")
    print("The dbt service will compile the project and generate the manifest.json file.")
    dbt_assets = None 
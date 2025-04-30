from dagster_loc import job
from .ops import (
    generate_nike_data_op,
    upload_raw_data_to_minio_op,
    load_raw_data_to_duckdb_op,
    run_dbt_models_op # Assuming you created this
)
from .resources import minio_resource, duckdb_resource, DataGenerationConfig, my_dbt_cli_resource # Import dbt resource

@job(
    resource_defs={
        "minio_resource": minio_resource,
        "duckdb_resource": duckdb_resource,
        "data_gen_config": DataGenerationConfig(output_dir="/app/data-generator/nike_data", metadata_file="/app/data-generator/nike_data/metadata.json"), # Use paths inside the dagster container
        "dbt": my_dbt_cli_resource.configured({
            "project_dir": "/dbt", # Matches docker-compose volume mount
            "profiles_dir": "/dbt", # Matches docker-compose volume mount
        })
    }
)
def nike_data_pipeline():
    """
    The main data pipeline for generating, loading, and transforming Nike data.
    """
    # Ensure MinIO bucket exists (optional, can be run separately or handled in resource init)
    # ensure_minio_bucket_op() # If you create this op

    # 1. Generate data locally
    generated_files = generate_nike_data_op()

    # 2. Upload raw data to MinIO
    uploaded_files_info = upload_raw_data_to_minio_op(generated_files=generated_files)

    # 3. Load raw data from MinIO into DuckDB (raw layer)
    # This op now implicitly loads the latest based on MinIO contents and merges
    raw_load_complete = load_raw_data_to_duckdb_op(uploaded_files_info=uploaded_files_info)

    # 4. Run dbt models to transform data (staging to silver)
    # The dbt op depends on the raw data being loaded
    run_dbt_models_op(start_after=raw_load_complete)
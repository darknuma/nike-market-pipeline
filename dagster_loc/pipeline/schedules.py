from dagster_loc import schedule
from .jobs import nike_data_pipeline

@schedule(cron_schedule="0 * * * *", job=nike_data_pipeline, execution_timezone="UTC")
def hourly_nike_data_schedule(context):
    """
    Schedule to run the Nike data pipeline hourly.
    """
    run_config = {
        "ops": {
            "generate_nike_data_op": {
                "config": {
                    "batch_size": 500 # Example: run smaller batches hourly
                }
            }
        }
    }
    return run_config
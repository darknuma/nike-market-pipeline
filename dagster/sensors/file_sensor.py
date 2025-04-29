from dagster import RunRequest, sensor, SensorEvaluationContext
from datetime import datetime, timedelta

@sensor(job_name="process_new_data")
def new_file_sensor(context: SensorEvaluationContext):
    """
    Sensor that monitors for new files in the specified directory
    and triggers the process_new_data job when new files are detected.
    """
    # TODO: Implement the actual file monitoring logic
    # This is a placeholder that triggers the job every minute
    if datetime.now().second == 0:
        yield RunRequest(
            run_key=f"new_file_{datetime.now().isoformat()}",
            run_config={
                "ops": {
                    "process_new_file": {
                        "config": {
                            "file_path": "path/to/new/file"
                        }
                    }
                }
            }
        ) 
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "start_date": datetime(2023, 1, 1),
}

default_task_sensor_args = {
    "allowed_states": ["success"],
    "failed_states": ["failed", "skipped"],
    "check_existence": True,
    "poke_interval": 10,
    "mode": "reschedule",
    "timeout": 3600,
}

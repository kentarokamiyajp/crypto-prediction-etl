import sys
import pytz
from datetime import datetime

sys.path.append("/opt/airflow")
from dwh_modules.common.utils import send_line_message


def send_line_notification(
    dag_id: str, tags: list, type: str, optional_message: str = None, context=None
):
    """Send a notification via LINE API

    Args:
        context (_type_): Airflow context
        dag_id (str): Dag id
        tags (list): Tags of the Dag
        type (str): Notification type
        optional_message (str, optional): Message body for the notification. Defaults to None.
    """
    jst = pytz.timezone("Asia/Tokyo")
    ts_now = datetime.now(jst).strftime("%Y-%m-%d %H:%M:%S")
    message = "{} [{}]{}\nAirflow Dags: {}".format(ts_now, ",".join(tags), type, dag_id)
    if optional_message:
        message += "\n\n" + optional_message
    send_line_message(message)

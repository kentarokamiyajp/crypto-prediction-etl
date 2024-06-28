import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__)))
from datetime import datetime, timedelta, date
from common_functions.notification import send_line_notification

sys.path.append("/opt/airflow")
from dwh_modules.cassandra_operations import cassandra_operator
from dwh_modules.common.utils import is_market_open


def batch_insertion_from_xcom(**kwargs) -> None:
    """Batch insertion for XCOM data from Airflow"""
    cass_op = cassandra_operator.CassandraOperator()
    keyspace = kwargs["keyspace"]
    query = kwargs["query"]
    batch_size = kwargs["batch_size"]
    batch_data = kwargs["ti"].xcom_pull(task_ids=kwargs["task_id_for_xcom_pull"])

    cass_op.batch_insertion(keyspace, batch_size, query, batch_data)


def check_latest_dt(
    dag_id: str,
    tags: list,
    keyspace: str,
    table_name: str,
    target_id_value: str,
    target_market_to_check_opening: str = None,
) -> None:
    """Check if the latest data exists in Cassandra

    Args:
        dag_id (str): Dag id
        tags (list): Tags for the DAG
        keyspace (str): target keyspace including the target table
        table_name (str): target table name
        target_id_value (str): target value for the "id" column
        target_market_to_check_opening (str): market name to check if the market is open on the day
    """

    cass_op = cassandra_operator.CassandraOperator()
    prev_date_ts = datetime.today() - timedelta(days=1)
    prev_date = date(prev_date_ts.year, prev_date_ts.month, prev_date_ts.day).strftime("%Y-%m-%d")

    query = f"""
    select count(*) from {table_name} where dt_create_utc = '{prev_date}' and id = '{target_id_value}'
    """

    count = cass_op.run_query(keyspace, query).one()[0]

    # If there is no data for prev-day even on the market holiday, exit with error.
    if int(count) == 0:
        if target_market_to_check_opening != None and is_market_open(
            prev_date, target_market_to_check_opening
        ):
            warning_message = "There is no data for prev_date ({}, asset:{})".format(
                prev_date, target_id_value
            )
            send_line_notification(
                dag_id=dag_id, tags=tags, type="WARNING", optional_message=warning_message
            )
        else:
            warning_message = "There is no data for prev_date ({}, asset:{})".format(
                prev_date, target_id_value
            )
            send_line_notification(
                dag_id=dag_id, tags=tags, type="ERROR", optional_message=warning_message
            )

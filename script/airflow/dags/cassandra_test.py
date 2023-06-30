from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from cassandra.cluster import Cluster
import pprint

cassandra_hook = CassandraHook("cassandra_default")
pp = pprint.PrettyPrinter(indent=4)

dag = DAG("Airflow_and_Cassandra",
    description="A simple DAG that help to process data from API and send data to Cassandra Database",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["ETL_jobs"])

def insert_into_cassandra_db():
    cluster = Cluster(['172.29.0.3'], port = 9042)
    session = cluster.connect('crypto')

    session.execute(
    """
    SELECT COUNT(*) FROM %s
    """,('crypto.coins_markets')
    )


insert_into_cassandra = PythonOperator(
    task_id="cassandra_select",
    python_callable=insert_into_cassandra_db,
    dag=dag,
)

insert_into_cassandra

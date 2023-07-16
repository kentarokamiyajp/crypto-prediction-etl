import sys

sys.path.append("/opt/airflow/git/crypto_prediction_dwh/script/")
from cassandra_operations import cassandra_operator


def insert_data(keyspace, candle_data, query):
    cass_ope = cassandra_operator.Operator(keyspace)

    batch_size = 100
    curr_index = 0
    while curr_index < len(candle_data):
        cass_ope.insert_batch_data(query, candle_data[curr_index : curr_index + batch_size])
        curr_index += batch_size

    curr_index -= batch_size
    cass_ope.insert_batch_data(query, candle_data[curr_index:])


def check_latest_dt(keyspace, query):
    cass_ope = cassandra_operator.Operator(keyspace)
    res = cass_ope.run_query(query)

    return res


def create_table(keyspace, query):
    cass_ope = cassandra_operator.Operator(keyspace)
    cass_ope.run_query(query)

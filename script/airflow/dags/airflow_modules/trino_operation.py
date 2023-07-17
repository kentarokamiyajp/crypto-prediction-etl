import sys

sys.path.append("/opt/airflow/git/crypto_prediction_dwh/script/")
from trino_operations import trino_operator


def run(query):
    user = "trino"
    trino_ope = trino_operator.Operator(user)
    res = trino_ope.run_query(query)
    return res

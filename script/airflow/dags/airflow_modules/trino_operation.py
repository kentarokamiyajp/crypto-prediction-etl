import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__)))

import airflow_env_variables

sys.path.append(airflow_env_variables.DWH_SCRIPT)
from trino_operations import trino_operator


def run(query):
    trino_ope = trino_operator.Operator()
    res = trino_ope.run_query(query)
    return res

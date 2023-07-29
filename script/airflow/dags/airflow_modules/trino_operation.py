import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__)))

import airflow_env_variables

sys.path.append(airflow_env_variables.DWH_SCRIPT)
from trino_operations import trino_operator


def run(query):
    user = airflow_env_variables.TRINO_USER
    trino_ope = trino_operator.Operator(user)
    res = trino_ope.run_query(query)
    return res


if __name__ == "__main__":
    query_file = (
        "/opt/airflow/dags/query_script/trino/init/OT_Load_stock_index_value_day.sql"
    )
    with open(query_file, "r") as f:
        query = f.read()
    print(run(query))

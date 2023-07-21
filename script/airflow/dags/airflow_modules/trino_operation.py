import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__)))

import env_variables

sys.path.append(env_variables.DWH_SCRIPT)
from trino_operations import trino_operator


def run(query):
    user = "trino"
    trino_ope = trino_operator.Operator(user)
    res = trino_ope.run_query(query)
    return res

import os, sys
from trino.dbapi import connect

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import env_variables


class TrinoOperator:
    def __init__(self, catalog=None, schema=None):
        self.conn = connect(
            host=env_variables.TRINO_HOST,
            port=int(env_variables.TRINO_PORT),
            user=env_variables.TRINO_USER,
            catalog=catalog,
            schema=schema,
        )
        self.cur = self.conn.cursor()

    def run_query(self, query):
        print(f"RUN QUERY\n{query}")
        self.cur.execute(query)
        return self.cur.fetchall()

from trino.dbapi import connect
from common import env_variables


class Operator:
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
        self.cur.execute(query)
        return self.cur.fetchall()

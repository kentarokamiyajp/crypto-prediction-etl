from trino.dbapi import connect
from pprint import pprint
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


if __name__ == "__main__":
    user = env_variables.TRINO_USER
    catalog = "hive"
    cursor = Operator()
    
    query = """
    with sample as (select max(dt) as max_dt from hive.crypto_raw.candles_day) select max_dt from sample
    """

    res = cursor.run_query(query)
    pprint(res)

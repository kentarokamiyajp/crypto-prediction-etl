from trino.dbapi import connect
from pprint import pprint
from modules import env_variables

conn = connect(
    host=env_variables.TRINO_HOST,
    port=int(env_variables.TRINO_PORT),
    user=env_variables.TRINO_USER,
    catalog="cassandra",
    schema="crypto",
)
cur = conn.cursor()
cur.execute("SELECT * FROM system.runtime.nodes")
rows = cur.fetchall()

pprint(rows)

from trino.dbapi import connect
from pprint import pprint

conn = connect(
    host="192.168.10.14",
    port=8881,
    user="trino",
    catalog="cassandra",
    schema="crypto",
)
cur = conn.cursor()
cur.execute("SELECT * FROM system.runtime.nodes")
rows = cur.fetchall()

pprint(rows)

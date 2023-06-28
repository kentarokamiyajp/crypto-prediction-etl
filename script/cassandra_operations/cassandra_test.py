from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pprint import pprint

auth_provider = PlainTextAuthProvider(username='kamiken', password='kamiken')

cluster = Cluster(['172.29.0.11'], port=9042, auth_provider=auth_provider)
session = cluster.connect()

row = session.execute("SELECT count(*) FROM crypto.coins_by_id where id='bitcoin'", timeout=60*60*10).one()
pprint(row)
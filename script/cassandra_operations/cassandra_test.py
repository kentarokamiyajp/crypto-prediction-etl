from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pprint import pprint
from modules import env_variables

auth_provider = PlainTextAuthProvider(
    username=env_variables.CASSANDRA_USERNAME, password=env_variables.CASSANDRA_PASSWORD
)

cluster = Cluster(
    [env_variables.CASSANDRA_HOST],
    port=int(env_variables.CASSANDRA_HOST),
    auth_provider=auth_provider,
)
session = cluster.connect()

row = session.execute(
    "SELECT count(*) FROM crypto.coins_by_id where id='bitcoin'", timeout=60 * 60 * 10
).one()
pprint(row)

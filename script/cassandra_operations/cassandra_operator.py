import sys
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, BatchStatement


class Operator:
    def __init__(self, keyspace):
        auth_provider = PlainTextAuthProvider(username="kamiken", password="kamiken")

        self.cluster = Cluster(["172.29.0.2"], port=9042, auth_provider=auth_provider)
        self.session = self.cluster.connect(keyspace)

    def __del__(self):
        self.session.shutdown()
        self.cluster.shutdown()

    def run_query(self, query):
        res = self.session.execute(query, timeout=60 * 60 * 10)
        return res

    def count_expected_variables(self, query, target_data):
        if len(target_data) != query.count("%s"):
            print("Expected {} %s in the query, but got {} %s".format(len(target_data), query.count("%s")))
            sys.exit(1)

    def insert_single_data(self, query, data):
        self.count_expected_variables(query, data)
        self.session.execute(query, tuple(data))

    def insert_batch_data(self, query, batch_data):
        self.count_expected_variables(query, batch_data[0])
        batch = BatchStatement()
        for data in batch_data:
            batch.add(SimpleStatement(query), tuple(d for d in data))
        self.session.execute(batch)

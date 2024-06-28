import sys, os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, BatchStatement

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import env_variables


class CassandraOperator:
    def __init__(self):
        auth_provider = PlainTextAuthProvider(
            username=env_variables.CASSANDRA_USERNAME,
            password=env_variables.CASSANDRA_PASSWORD,
        )

        self.cluster = Cluster(
            [env_variables.CASSANDRA_HOST],
            port=int(env_variables.CASSANDRA_PORT),
            auth_provider=auth_provider,
        )

    def __del__(self):
        self.cluster.shutdown()

    def _create_session(self, keyspace):
        return self.cluster.connect(keyspace)

    def _check_num_of_variables_in_query(self, query: str, data: list):
        """Check if number of variables matches

        Args:
            query (str): query string
            data (list): batch data used for the query
        """
        if len(data) != query.count("%s"):
            print("Expected {} %s in the query, but got {} %s".format(len(data), query.count("%s")))
            sys.exit(1)

    def run_query(self, keyspace: str, query: str):
        """Run query

        Args:
            keyspace (str): cassandra keyspace name to connect
            query (str): query string

        Returns:
            _type_: query results
        """

        print(f"RUN QUERY\n{query}")

        with self._create_session(keyspace) as sess:
            return sess.execute(query, timeout=60 * 60 * 10)

    def non_batch_insertion(self, keyspace: str, query: str, target_data: list):
        """Data insertion (not batch operation)

        Args:
            keyspace (str): cassandra keyspace name to connect
            query (str): query string
            target_data (list): target data used for the query
        """

        print(f"RUN QUERY\n{query}")

        with self._create_session(keyspace) as sess:
            self._check_num_of_variables_in_query(query, target_data)
            sess.execute(query, tuple(target_data))

    def batch_insertion(self, keyspace: str, batch_size: int, query: str, target_data: list):
        """Batch data insertion

        Args:
            keyspace (str): cassandra keyspace name to connect
            batch_size (int): batch size
            query (str): query string
            target_data (list): target data used for the query
                the target data will be divided into batch chunk
        """

        print(f"RUN QUERY\n{query}")

        def _execute(curr_index):
            batch = BatchStatement()
            batch_data = target_data[curr_index : curr_index + batch_size]

            self._check_num_of_variables_in_query(query, batch_data[0])

            for data in batch_data:
                batch.add(SimpleStatement(query), tuple(d for d in data))

            sess.execute(batch)

        with self._create_session(keyspace) as sess:
            curr_index = 0
            while curr_index < len(target_data):
                _execute(curr_index)
                curr_index += batch_size

            # exec for the remaining data
            curr_index -= batch_size
            _execute(curr_index)

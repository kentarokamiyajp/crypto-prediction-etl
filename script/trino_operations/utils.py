from trino.dbapi import connect
from pprint import pprint

class Operator:
    def __init__(self, user, catalog, schema=None):
        self.conn = connect(
            host="192.168.10.14",
            port=8881,
            user=user,
            catalog=catalog,
            schema=schema,
        )
        self.cur = self.conn.cursor()
    
    def create_schema(self, schema):
        self.cur.execute(f"CREATE SCHEMA {schema}")
        
    def run_query(self, query):
        self.cur.execute(query)
        return self.cur.fetchall()

if __name__=="__main__":
    user = 'trino'
    catalgo = 'hive'
    cursor = Operator(user, catalgo)
    
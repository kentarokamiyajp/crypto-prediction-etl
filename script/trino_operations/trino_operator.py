from trino.dbapi import connect
from pprint import pprint

class Operator:
    def __init__(self, user, catalog=None, schema=None):
        self.conn = connect(
            host="192.168.10.14",
            port=8881,
            user=user,
            catalog=catalog,
            schema=schema,
        )
        self.cur = self.conn.cursor()
        
    def run_query(self, query):
        self.cur.execute(query)
        return self.cur.fetchall()

if __name__=="__main__":
    user = 'trino'
    catalog = 'hive'
    cursor = Operator(user,)

    query = f"""
    insert into hive.crypto_raw.candles_minute
    (id,low,high,open,close,amount,quantity,buyTakerAmount,buyTakerQuantity,tradeCount,ts,weightedAverage,interval_type,startTime,closeTime,dt,ts_insert_utc,year, month, day, hour)
    values ('id_1',1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1,1000000,1.0,'b',1000000,100000000,cast('2023-03-03' as date),cast('2023-03-03 22:22:22' as timestamp),2023,03,03,11)
    """
    
    query = f"""
    insert into hive.crypto_raw.candles_minute
    (id,low,high,open,close,amount,quantity,buyTakerAmount,buyTakerQuantity,tradeCount,ts,weightedAverage,interval_type,startTime,closeTime,dt,ts_insert_utc,year, month, day, hour)
    select
    id,low,high,open,close,amount,quantity,buyTakerAmount,buyTakerQuantity,tradeCount,ts,weightedAverage,interval,startTime,closeTime,dt,ts_insert_utc
    , year(from_unixtime(closeTime))
    , month(from_unixtime(closeTime))
    , day(from_unixtime(closeTime))
    , hour(from_unixtime(closeTime))
    from cassandra.crypto.candles_minute_realtime
    """
    
    query = """
    with sample as (select max(dt) as max_dt from hive.crypto_raw.candles_day) select max_dt from sample
    """
    
    res = cursor.run_query(query)
    pprint(res)
    
from dwh_script.cassandra_operations import cassandra_operator


def insert_data(keyspace, table_name, candle_data):
    cass_ope = cassandra_operator.Operator(keyspace)

    batch_size = 100
    curr_index = 0
    while curr_index < len(candle_data):
        insert_query = f"""
        INSERT INTO {table_name} (id,low,high,open,close,amount,quantity,buyTakerAmount,\
            buyTakerQuantity,tradeCount,ts,weightedAverage,interval,startTime,closeTime,dt,ts_insert_utc)\
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cass_ope.insert_batch_data(insert_query, candle_data[curr_index : curr_index + batch_size])
        curr_index += batch_size

    curr_index -= batch_size
    cass_ope.insert_batch_data(insert_query, candle_data[curr_index:])


def check_latest_dt(keyspace, table_name, target_date, target_asset):
    cass_ope = cassandra_operator.Operator(keyspace)

    query = f"""
    select count(*) from {table_name} where dt = '{target_date}' and id = '{target_asset}'
    """

    res = cass_ope.run_query(query)

    return res

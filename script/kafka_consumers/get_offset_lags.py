import multiprocessing
import time

from confluent_kafka import Consumer, TopicPartition


def get_partition_lag(partition: int):
    topic_name = "crypto.order_book"
    CONFLUENT_CONFIG = {
        "bootstrap.servers": '192.168.10.4:9081,192.168.10.4:9082,192.168.10.4:9083',
        'group.id': 'order-book-consumer',
        'enable.auto.commit':False
    }
    consumer = Consumer(CONFLUENT_CONFIG)
    partition_lag = {}
    print(f"Getting lag for topic: {topic_name}, partition: {partition}")
    topic = TopicPartition(topic_name, partition)
    consumer.assign([topic])
    committed = consumer.committed([topic])[0].offset
    last_offset = consumer.get_watermark_offsets(topic)[1]
    print(consumer.get_watermark_offsets(topic))
    if committed < 0:
        return {}
    partition_lag[partition] = last_offset - committed
    print(f"Partition: {partition}, lag:{last_offset-committed}")
    consumer.close()
    return partition_lag


if __name__ == "__main__":
    topic_wise_lag = {}
    paritition_count = 3

    t0 = time.perf_counter()
    pool = multiprocessing.Pool(processes=paritition_count)

    inputs = [x for x in range(paritition_count)]

    outputs = pool.map(get_partition_lag, inputs)
    print(f"Time taken: {time.perf_counter()-t0}")

    for output in outputs:
        topic_wise_lag.update(output)
    
    print(topic_wise_lag)

    max_lag = max(zip(topic_wise_lag.values(), topic_wise_lag.keys()))[1]

    print(f"Max Lag: {topic_wise_lag[max_lag]}")
    print(f"Total Unconsumed: {sum(topic_wise_lag.values())}")
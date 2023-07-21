from confluent_kafka import Consumer
from modules import env_variables

################
c = Consumer(
    {
        "bootstrap.servers": env_variables.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "python-consumer",
        "auto.offset.reset": "earliest",
    }
)
print("Kafka Consumer has been initiated...")

print("Available topics to consume: ", c.list_topics().topics)
c.subscribe(["crypto.coins_list"])


################
def main():
    while True:
        msg = c.poll(1.0)  # timeout
        if msg is None:
            continue
        if msg.error():
            print("Error: {}".format(msg.error()))
            continue
        data = msg.value().decode("utf-8")
        print(data)
    c.close()


if __name__ == "__main__":
    main()

from confluent_kafka import Producer
import json
import time
from common import env_variables

####################
p = Producer({"bootstrap.servers": env_variables.KAFKA_BOOTSTRAP_SERVERS})
print("Kafka Producer has been initiated...")


#####################
def receipt(err, msg):
    if err is not None:
        print("Error: {}".format(err))
    else:
        message = "Produced message on topic {} with value of {}\n".format(
            msg.topic(), msg.value().decode("utf-8")
        )
        print(message)


#####################
def main():
    while True:
        price = {"BTC": 100.00}
        m = json.dumps(price)
        p.produce("sample.topic", m.encode("utf-8"), callback=receipt)
        p.flush()
        print("Price sent to consumer")
        time.sleep(5)


if __name__ == "__main__":
    main()

from confluent_kafka import Producer
import json
import time
import logging
import random
import requests

url = 'https://api.coinbase.com/v2/prices/btc-usd/spot'

logdir = '/home/kamiken/kafka/python_log'
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename=f'{logdir}/producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

####################
p=Producer({'bootstrap.servers':'192.168.240.5:9092'})
print('Kafka Producer has been initiated...')
#####################
def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)
        
#####################
def main():
    while(True):
        price = ((requests.get(url)).json())
        print("Price fetched")
        print(price)
        m=json.dumps(price)
        p.produce('sample.topic', m.encode('utf-8'),callback=receipt)
        p.flush()
        print("Price sent to consumer")
        time.sleep(5)
            
if __name__ == '__main__':
    main()
import os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from confluent_kafka import Producer
import json
import time
import datetime
import logging
import random
import requests
from pprint import pprint
from coingecko_apis.crypto_api import CryptoOperator

args = sys.argv
curr_date = args[1]
curr_timestamp = args[2]
print(curr_date,curr_timestamp)
logdir = f'/home/kamiken/kafka/log/{curr_date}'

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename=f'{logdir}/coins_realtime_producer_{curr_timestamp}.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(10)

co = CryptoOperator()

kafka_server = "172.29.0.13:9092"

# set a producer
p=Producer({'bootstrap.servers':kafka_server})
logger.info('Kafka Producer has been initiated...')

def receipt(err,msg):
    if err is not None:
        logger.error('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        # logger.info(message)


def main():    
    retry_count = 0
    loop_count = 0
    loop_max_count = 100
    while(True):
        # coins_markets
        logger.info("Get coins_markets")
        try:
            coins_markets = co.get_coins_markets(vs_currency='usd',order='market_cap_desc',per_page='100',page='1')
            logger.info('Success to get the coins market data')
            time.sleep(60)
            retry_count = 0
        except:
            retry_count+=1
            logger.warning('Could not get the coins market data')
            logger.warning(f'Retry: {retry_count}')
            time.sleep(10)
            
        coins_markets = {"data":coins_markets}
        m=json.dumps(coins_markets)
        # logger.info("Send the coins_market data to Kafka topic")
        p.produce('crypto.coins_markets', m.encode('utf-8'),callback=receipt)
                
        # for coins in coins_markets["data"]:
        #     # logger.info("Get {} current data".format(coins['id']))
        #     coin_data = co.get_coins_by_id(id=coins['id'],tickers=True,market_data=True)
        #     m=json.dumps(coin_data)
        #     # logger.info("Send the {} data to Kafka topic".format(coins['id']))
        #     p.produce('crypto.coins_by_id', m.encode('utf-8'),callback=receipt)
        #     loop_count+=1
        #     if loop_count == loop_max_count:
        #         loop_count = 0
        #         p.flush()
        #     time.sleep(10)
            
if __name__ == '__main__':
    main()
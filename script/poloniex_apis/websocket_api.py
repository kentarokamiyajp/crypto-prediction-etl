import asyncio
import json
import websockets

"""Poloniex Websocket API
A single IP is limited to 2000 simultaneous connections on each of the public and private channels.
Once a connection has been established, each connection is limited to 500 requests per second.

Subscription usege:
    data_to_send = {
        "event": "subscribe",
        "channel": ["<channel>"],
        "symbols": [
            "<symbol1>",
            "<symbol2>",
            "<symbol3>"
        ]
    }
    
Multiple channel subscription:
    data_to_send = {
        "event": "subscribe",
        "channel": ["orders","balances"],
        "symbols": ["all"]
    }
"""

class PoloniexOperator:
    def __init__(self):
        self.public_uri = "wss://ws.poloniex.com/ws/public"
        self.private_uri = "wss://ws.poloniex.com/ws/private"
        
    def default_process_data_for_kafka(self, data):
        return data
        
    async def send_request(self, data_to_send, request_interval, transfer_to_kafka=False, send_to_kafka_topic=None):
        
        if transfer_to_kafka == True and send_to_kafka_topic == None:
            print("ERROR: 'send_to_kafka_topic' function is not defined !!!")
            sys.exit(1)
        
        max_retry_count = 5
        curr_retry_count = 0
        while curr_retry_count <= max_retry_count:
            try:
                async with websockets.connect(self.public_uri, ping_interval=None) as websocket:
                    while True:
                        await websocket.send(json.dumps(data_to_send))
                        response = await websocket.recv()
                        
                        """response example
                        {"channel":"book","data":[{"symbol":"SHIB_USDT","createTime":1691907592627,"asks":[["0.000010583","240836"],["0.000010584","28068200"],["0.000010585","93619849"],["0.00001059","11098962"],["0.000010594","943930526"],["0.000010596","149582654"],["0.000010599","277246085"],["0.000010605","277246085"],["0.000010619","94170826"],["0.000010625","30000000"],["0.000010629","924551037"],["0.000010655","924551037"],["0.000010665","4278194171"],["0.000010725","374681163"],["0.000010766","500000"],["0.00001077","500000"],["0.000010772","500000"],["0.000010795","500000"],["0.000010805","1000000"],["0.00001081","184469988"]],"bids":[["0.000010582","6899684"],["0.000010571","975078449"],["0.00001057","290941531"],["0.000010564","500000"],["0.000010563","93619849"],["0.000010561","94782"],["0.000010558","149582654"],["0.000010552","277246085"],["0.00001055","94881"],["0.000010546","277246085"],["0.000010545","30000000"],["0.000010542","94858661"],["0.000010535","2327532252"],["0.000010534","924551037"],["0.000010507","475873"],["0.000010506","924551037"],["0.0000105","95333"],["0.000010496","4808708"],["0.000010495","3603556991"],["0.000010478","374681163"]],"id":133790392,"ts":1691907592696}]}
                        
                        symbol	String	symbol name
                        createTime	Long	time the record was created
                        asks	List<String>	sell orders, in ascending order of price
                        bids	List<String>	buy orders, in descending order of price
                        id	Long	id of the record (SeqId)
                        ts	Long	send timestamp
                        """
                        
                        if transfer_to_kafka and "data" in response:
                            send_to_kafka_topic(json.loads(response))
                            
                        await asyncio.sleep(request_interval)
                        curr_retry_count = 0 # reset
                        
            except websockets.ConnectionClosed as e:
                print(f"Connection closed with code {e.code}. Reconnecting...")
                await asyncio.sleep(5)
                curr_retry_count+=1

    def main_send_request(self, data_to_send, request_interval, transfer_to_kafka=False, send_to_kafka_topic=None):
        asyncio.get_event_loop().run_until_complete(self.send_request(data_to_send, request_interval, transfer_to_kafka, send_to_kafka_topic))
    
    
if __name__=="__main__":
    ws = PoloniexOperator()
    
    data_to_send = {
        "event": "subscribe", # event type: ping, pong, subscribe, unsubscribe, unsubscribe_all, list_subscriptions
        "channel": ["book"],
        "symbols": ["ADA_USDT",
                    "BCH_USDT",
                    "BNB_USDT",
                    "BTC_USDT",
                    "DOGE_USDT",
                    "ETH_USDT",
                    "LTC_USDT",
                    "MKR_USDT",
                    "SHIB_USDT",
                    "TRX_USDT",
                    "XRP_USDT"],
        "depth": 20
        }
    
    request_interval = 1
    
    ws.main_send_request(data_to_send, request_interval)
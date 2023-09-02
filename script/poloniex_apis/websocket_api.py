import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import websocket
import json
import traceback
from kafka_producers.producer_operation import KafkaProducer

class PoloniexSocketOperator:
    def __init__(self, connection_type: str, request_data: dict, send_kafka: bool, kafka_config: dict):
        # #Display debug log
        # websocket.enableTrace(True)
        
        public_uri = "wss://ws.poloniex.com/ws/public"
        private_uri = "wss://ws.poloniex.com/ws/private"
        
        def on_open(wsapp):
            """
            Do something when connection is established.
            """
            wsapp.send(request_data["subscribe_payload"])
            
        def on_message(wsapp, message):
            """
            Do something when getting a message from a server.
            """
            if "pong" not in message:
                # Send ping to keep connection live
                wsapp.send(request_data["ping_payload"])
                
                if "data" in message:
                    self._send_message_to_kafka(json.loads(message))
            
        def on_pong(wsapp, message):
            """
            Do something when getting a pong (response for a ping from client) from a server.
            """
            # print("Got a pong! No need to respond")
            pass
        
        try:
            if connection_type == "public":
                self.wsapp = websocket.WebSocketApp(public_uri, on_message=on_message, on_pong=on_pong, on_open=on_open)
            else:
                self.wsapp = websocket.WebSocketApp(private_uri, on_message=on_message, on_pong=on_pong, on_open=on_open)
        except:
            traceback.format_exc()
            sys.exit(1)
            
        if send_kafka:
            self.curr_date = kafka_config["curr_date"]
            self.curr_timestamp = kafka_config["curr_timestamp"]
            self.producer_id = kafka_config["producer_id"]
            self.topic_id = kafka_config["topic_id"]
            self.num_partitions = kafka_config["num_partitions"]
            self.func_process_response = kafka_config["func_process_response"]
            
            self.kafka_producer = KafkaProducer(self.curr_date, self.curr_timestamp, self.producer_id)

    def run_forever(self):
        self.wsapp.run_forever(reconnect=1) 
        
    def _send_message_to_kafka(self, response):
        message = self.func_process_response(response)
        self.kafka_producer.produce_message(
            self.topic_id, json.dumps(message), int(self.num_partitions)
        )
        self.kafka_producer.poll_message(timeout=10)

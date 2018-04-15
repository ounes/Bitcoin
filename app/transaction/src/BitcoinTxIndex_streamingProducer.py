from kafka import KafkaProducer

import json
import logging

from websocket import create_connection

def produce_Tx_Index(topic):
    """  Get the current information of transaction by creating a connection to "ws://ws.blockchain.info/inv" and seed it to Kafka.

    Arguments:
        topic {string} -- Name of topic to Produce in Kafka

    Returns:
        Void -- Server
    """
    producer = KafkaProducer(acks=1, max_request_size=10000000, bootstrap_servers='kafka:9092')
    ws = create_connection("ws://ws.blockchain.info/inv")

    while True:
        ws.send(json.dumps({"op": "unconfirmed_sub"}))
        tx=ws.recv()
        producer.send(topic,str(tx).encode())
        logging.info("SEND")

if __name__ == "__main__":
    produce_Tx_Index("transaction_str")

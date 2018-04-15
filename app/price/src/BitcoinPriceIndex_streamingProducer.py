import logging
import sys
import json
import time
import random

from kafka import KafkaProducer
from kafka.errors import KafkaError

from elastic_storage import getCurrentPrice, createCurrentDataset 

def send_to_consumer(producer):
    """
    Send current price to SparkStreaming and is "client-crush"-proof.

    Arguments:
        current {string} -- [description]
        tcp_connection {string} -- [description]
        s {socket} -- socket of connection

    Returns:
        [void] -- []
    """
    while True:
        last_current = createCurrentDataset(getCurrentPrice())
        producer.send('price_str',str(last_current).encode())
        logging.info("INFO")
        print(last_current)
        time.sleep(50)
    producer.close()

if __name__ == "__main__":
    from config import config
    producer = KafkaProducer(acks=1,max_request_size=10000000,bootstrap_servers='kafka:9092')
    send_to_consumer(producer)

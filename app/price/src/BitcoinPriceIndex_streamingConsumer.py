import logging
import ast
import datetime

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

from pyspark.streaming.kafka import KafkaUtils

from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl.connections import connections

from elastic_storage import storeData, BitCoin, eraseData, http_auth

TIME_FORMAT = '%Y-%m-%dT%H:%M:00'

def send(rdd):
    """
     Send the rdd, that's an information passed at argument of "send" function, to our elastic database.

    Arguments:
        rdd {pyspark.RDD} -- [description]
        host {string} -- [description]

    Returns:
        [void] -- []
    """
    data_tx=rdd.collect()
    if data_tx:
        date = data_tx[0]['date']
        value = data_tx[0]['value']
        storeData(date, float(value), "real-time")

def streamingPrice(config, master="local[2]", appName="CurrentPrice", group_id='Alone-In-The-Dark', topicName='price_str', producer_host="zookeeper", producer_port='2181'):
    """
    Create a Spark Streaming which listening in hostname:port, get a text from a socket server and then print it and send it to our elastic data base every 60 secondes.

    Arguments:
        master {string} -- [description]
        appName {string} -- [description]
        producer_host {string} -- [description]
        db_host {string} -- [description]
        port {int} -- [description]
    """
    sc = SparkContext(master, appName)
    strc = StreamingContext(sc, 60)
    logging.info("Connecting to host {0}:{1}".format(producer_host, producer_port))

    dstream = KafkaUtils.createStream(strc,producer_host+":"+producer_port,group_id,{topicName:1},kafkaParams={"fetch.message.max.bytes":"1000000000"})\
            .map(lambda v: ast.literal_eval(v[1]))

    dstream.pprint()
            
    dstream.foreachRDD(lambda rdd: send(rdd))

    strc.start()
    strc.awaitTermination()

def streamingPriceDict(config):
    streamingPrice(config)

if __name__ == "__main__":
    from config import config
    connections.create_connection(hosts=config['elasticsearch']['hosts'], http_auth=http_auth(config['elasticsearch']))
    streamingPrice(config)

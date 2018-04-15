from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import json
import datetime
import ast
import logging

from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch, helpers

from elastic_storage import http_auth

bitcoinToSatoshi=100000000
DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'

def add_real_time_tx(name,realTimeData):
    """
    Add the data contains in 'realTimeData' to ElasticSearchBase, represented by 'es', of table indice 'index'.

    Arguments:
        realTimeData { list[directories] } -- list of dictionnary
        index {string} -- name of indice of ElasticDataBase

        Returns:
        void -- Put data to ElasticDataBase
    """
    actions = [
        {
            "_index": name,
            "_type": "doc",
            "_id": int(data['tx_index']),
            "_source":{
                "type": "real-time",
                "value": float(data['value'])/bitcoinToSatoshi,
                "time": {
                    'path': data['time'], 
                    'format': DATE_FORMAT
                    }
            }
        }
        for data in realTimeData
    ]
    logging.info("Bulk contain {0} transactions".format(len(actions)))
    helpers.bulk(connections.get_connection(), actions)

def timestampsToString(timestamps):
    return str(datetime.datetime.fromtimestamp(int(timestamps)).strftime(DATE_FORMAT))

def filter_tx(dico):
    """ Filter just after the input of the streaming to get the time, the value and the tx_index of each transaction.

    Arguments:
        dico {dictionnary} -- contains many informations what need to be filtered

    Returns:
        dictionnary -- filtered liste of dictionnary
    """
    js=dico['x']
    result = [{
        'time': timestampsToString(js['time']),
        'tx_index':js['tx_index'],
        'value': sum( (js['inputs'][i]['prev_out']['value'] for i in list(range(len(js['inputs'])))) )
        }]

    return result

def send(index,rdd):
    
    data_tx = rdd.collect()
    if data_tx:
        add_real_time_tx(index,data_tx[0])
        logging.info("INFO")

def consume_Tx_Index(topic,index,master="local[2]", appName="CurrentTransaction",elasticsearch_host="db" ,elasticsearch_port=9200, kafkaConsumer_host="zookeeper",kafkaConsumer_port=2181):
    """ Get the current information of transaction by a KafkaProducer and SparkStreaming/KafkaUtils, theses informations are send to an ElasticSearchBase.

    Arguments:
        topic {string} -- Name of topic to Consume in Kafka
        index {string} -- Name of index of ElasticSearchDataBase
        master {string} -- Set master URL to connect to
        appName {string} -- Set application name
        elasticsearch_host {string} -- ElasticSearch host to connect for
        elasticsearch_port {int} -- ElasticSearch port to connect for
        kafkaConsumer_host {string} -- kafkaStream consumer to connect for getting streamingPrice
        kafkaConsumer_port {int} -- kafkaStream port to connect for

    Returns:
        void -- Send to ElasticSearchDataBase data.
    """
    sc = SparkContext(master, appName)
    strc = StreamingContext(sc, 5)

    kafkaStream = KafkaUtils.createStream(strc, kafkaConsumer_host+':'+str(kafkaConsumer_port), 'SparkStreaming', {topic: 1})\
            .map(lambda v: json.loads(v[1]))\
            .map(filter_tx)

    kafkaStream.pprint()
    
    kafkaStream.foreachRDD(lambda rdd: send(index,rdd))

    strc.start()
    strc.awaitTermination()

if __name__ == "__main__":
    from config import config
    connections.create_connection(hosts=config['elasticsearch']['hosts'], http_auth=http_auth(config['elasticsearch']))
    consume_Tx_Index('transaction_str','bitcoin_tx')

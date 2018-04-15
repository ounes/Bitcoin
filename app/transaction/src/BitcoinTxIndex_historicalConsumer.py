import datetime
import ast
import json
import logging

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch, helpers

from elastic_storage import http_auth

TIME_FORMAT = '%Y-%m-%dT%H:%M:%S'

def add_historical_tx(historicalDataset, satochiToBitcoin=100000000):
    ''' Get data from the API between two dates '''
    ''' Call to bulk api to store the data '''
    actions = [
        {
            "_index": "bitcoin_tx",
            "_type": "doc",
            "_id": data['id_tx'],
            "_source": {
                "type": "historical",
                "value": data['value']/satochiToBitcoin,
                "time": {'path': data['date'], 'format': TIME_FORMAT}
            }
        }
        for data in historicalDataset
    ]
    helpers.bulk(connections.get_connection(), actions)

def filter_tx(data):
    """ Filter the transactions information to keep only date, value and transaction id
    
    Arguments:
        data {list} -- All information for a block
    
    Returns:
        list -- Return only value, date and transaction id
    """

    tx_filter = []
    if data:
        for json_tx in data['tx']:
            if 'inputs' in json_tx.keys():
                current = {}
                current['id_tx'] = json_tx['tx_index']
                current['date'] = timestampToDate(json_tx['time'])
                current['value'] = 0
                for json_inputs in json_tx['inputs']:
                    if 'prev_out' in json_inputs.keys():
                        current['value'] += float(json_inputs['prev_out']['value'])
                current['value'] = current['value'] / len(json_tx['inputs'])
                tx_filter.append(current)
    return tx_filter

def timestampToDate(timestamp):
    """ Convert timestamp date to datetime date
    
    Arguments:
        timestamp {int} -- Timestamp date
    
    Returns:
        Datetime -- Datetime date
    """

    return datetime.datetime.fromtimestamp(
                int(timestamp)).strftime(TIME_FORMAT)

def send(rdd):
    """ Send to elastic
    
    Arguments:
        rdd {RDD} -- Data to send to elastic
    
    Keyword Arguments:
        config {dict} -- Configuration
    """

    data_tx = rdd.collect()
    if data_tx:
        add_historical_tx(data_tx[0])
        logging.info("INFO")

def HisticalTx(config, master="local[2]", appName="Historical Transaction", group_id='Alone-In-The-Dark', topicName='transaction_hist', producer_host="zookeeper", producer_port='2181', db_host="db"): 
    """ Load data from kafka, filter and send to elastic
    
    Keyword Arguments:
        config {dict} -- Contains Elasticsearch settings (hosts, password, ...)
        master {str} -- Master URL to connect to (default: {"local[2]"})
        appName {str} -- Application name (default: {"Historical Transaction"})
        group_id {str} -- Group id (default: {'Alone-In-The-Dark'})
        topicName {str} -- Topic name (default: {'transaction_hist'})
        producer_host {str} -- Producer host (default: {"localhost"})
        producer_port {str} -- Producer port (default: {'2181'})
        db_host {str} -- Database host (default: {"db"})
    """

    sc = SparkContext(master,appName)
    ssc = StreamingContext(sc,batchDuration=5)
    zkQuorum = "{0}:{1}".format(producer_host, producer_port)
    topics = { topicName: 1 }
    dstream = KafkaUtils.createStream(ssc, zkQuorum, group_id, topics, kafkaParams={"fetch.message.max.bytes":"1000000000"})\
                        .map(lambda v: ast.literal_eval(v[1]))\
                        .map(filter_tx)

    dstream.foreachRDD(lambda rdd: send(rdd))
    
    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    from config import config    
    connections.create_connection(hosts=config['elasticsearch']['hosts'], http_auth=http_auth(config['elasticsearch']))
    HisticalTx(config)

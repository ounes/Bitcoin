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

def add_historical_miners(historicalDataset, satochiToBitcoin=100000000):
    ''' Get data from the API between two dates '''
    ''' Call to bulk api to store the data '''
    actions = [
        {
            "_index": "bitcoin_miners",
            "_type": "doc",
            "_id": data['id_tx'],
            "_source": {
                "type": "historical",
                "time": {'path': data['date'], 'format': TIME_FORMAT},
                "value": data['value']/satochiToBitcoin,
                "addr": data["addr"]
            }
        }
        for data in historicalDataset
    ]
    helpers.bulk(connections.get_connection(), actions)

def filter_tx(tx):
    """ Filter the transaction information to keep only transaction id, date, value and addr of the output
    
    Arguments:
        tx -- First transaction of a block
    
    Returns:
        dict -- Return the transaction id, date, value and addr
    """

    tx_filtered = {}
    if tx:
        tx_filtered['id_tx'] = tx['tx_index']
        tx_filtered['date'] = timestampToDate(tx['time'])
        tx_filtered['value'] = tx['out'][0]['value']
        tx_filtered['addr'] = tx['out'][0]['addr']
    return tx_filtered

def timestampToDate(timestamp):
    """ Convert timestamp date to datetime date
    
    Arguments:
        timestamp {int} -- Timestamp date
    
    Returns:
        Datetime -- Datetime date
    """

    return datetime.datetime.fromtimestamp(
                int(timestamp)).strftime(TIME_FORMAT)

def send(rdd, config):
    """ Send to elastic
    
    Arguments:
        rdd {RDD} -- Data to send to elastic
    
    Keyword Arguments:
        config {dict} -- Configuration
    """

    data_tx = rdd.collect()
    if data_tx:
        add_historical_miners(data_tx)
        logging.info("Data sent to Elastic")

def HistoricalMiners(config, master="local[2]", appName="Historical Miners", group_id='Alone-In-The-Dark', topicName='miners_hist', producer_host="zookeeper", producer_port='2181', db_host="db"): 
    """ Load miners data from kafka, filter and send it to elastic
    
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
    dstream = KafkaUtils.createStream(ssc,producer_host+":"+producer_port,group_id,{topicName:1},kafkaParams={"fetch.message.max.bytes":"1000000000"})\
        .map(lambda v: ast.literal_eval(v[1]))\
        .map(filter_tx)

    dstream.pprint()

    dstream.foreachRDD(lambda rdd: send(rdd, config))
    
    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    from config import config
    connections.create_connection(hosts=config['elasticsearch']['hosts'], http_auth=http_auth(config['elasticsearch']))
    HistoricalMiners(config)

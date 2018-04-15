from websocket import create_connection
import datetime
import json

from elasticsearch_dsl.connections import connections
from elasticsearch import helpers, Elasticsearch

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from elastic_storage import eraseData, http_auth

WEBSOCKET_URL = "ws://ws.blockchain.info/inv"
WEBSOCKET_REQUEST = json.dumps({"op": "unconfirmed_sub"})
TIME_FORMAT = '%Y-%m-%dT%H:%M:%S'

def filter_tx(data):
    tx_filter = []
    if 'inputs' in data.keys():
        time = datetime.datetime.fromtimestamp(int(data['time'])).strftime(TIME_FORMAT)
        for json in data['inputs']:
            if 'prev_out' in json.keys():
                current = {}
                current['date'] = str(time)
                current['id_tx'] = json['prev_out']['tx_index']
                current['value'] = json['prev_out']['value']/100000000
                tx_filter.append(current)
    return tx_filter


def add_real_time_tx(realTimeData, conf):
    ''' Get data from the API between two dates '''
    ''' Call to bulk api to store the data '''
    actions = [
        {
            "_index": "bitcoin_tx",
            "_type": "doc",
            "_id": data['id_tx'],
            "_source":{
                "type": "real-time",
                "value": data['value'],
                "time": {'path': data['date'], 'format': TIME_FORMAT}
            }
        }
        for data in realTimeData
    ]
    helpers.bulk(connections.get_connection(), actions)

def getRealTimeTx(sc):
    ws = create_connection(WEBSOCKET_URL)
    ws.send(WEBSOCKET_REQUEST)
    rdd = sc.parallelize(json.loads(ws.recv()).items())\
            .filter(lambda js: type(js[1]) == dict)\
            .map(lambda js: filter_tx(js[1]))\
            .collect()
    return rdd[0]

def insert_real_time_tx(sc, conf):
    connections.create_connection(hosts=conf['elasticsearch']['hosts'], http_auth=http_auth(conf['elasticsearch']))
    #eraseData("real-time", "bitcoin_tx")
    while True:
        rdd = getRealTimeTx(sc)
        print(rdd)
        add_real_time_tx(rdd, conf['hosts'])

if __name__ == "__main__":
    from config import config 
    sc = SparkContext()
    insert_real_time_tx(sc, config)

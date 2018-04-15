import json
import time
import datetime
import logging

from http import client as httpClient
from http import HTTPStatus

from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch, helpers

from kafka import KafkaProducer
from kafka.errors import KafkaError

from pyspark import SparkContext

from elastic_storage import eraseData

DEFAULT_HOST = "blockchain.info"
URI_BLOCKS = "/fr/blocks/"
URI_TRANSACTIONS = "/fr/rawblock/"

def connectionToAPI(host, path):
    """ Connexion to the Blockchain API
    
    Arguments:
        host {string} -- API host
        path {string} -- API uri
    
    Returns:
        json -- Return the result of the API call
    """

    connection = httpClient.HTTPConnection(host)
    connection.request("GET", path)
    resp = connection.getresponse()
    result = {}
    if resp.status == HTTPStatus.OK:
        result = json.loads(resp.read().decode('utf-8'))
    else:
        time.sleep(10)
        connectionToAPI(host, path)
    connection.close()
    return result

def getListBlocks_1day(date, host=DEFAULT_HOST, uri=URI_BLOCKS):
    """ Get the list of blocks created for a date
    
    Arguments:
        date {string} -- Creation date of the block
    
    Keyword Arguments:
        host {string} -- API host (default: {DEFAULT_HOST})
        uri {string} -- API uri (default: {URI_BLOCKS})
    
    Returns:
        list -- Return all informations about blocks created on date
    """

    timestemp = int(time.mktime(datetime.datetime.strptime(
        date, "%Y-%m-%d").timetuple()))*1000
    path = uri + str(timestemp) + "?format=json"
    all_infos_blocks = connectionToAPI(host, path)
    return filter_listBlocks(all_infos_blocks)

def getListBlocks_Ndays(start, end, host=DEFAULT_HOST, uri=URI_BLOCKS):
    """ Get the list of blocks created between two dates
    
    Arguments:
        start {string} -- Start date
        end {string} -- End date
    
    Keyword Arguments:
        host {string} -- API host (default: {DEFAULT_HOST})
        uri {string} -- API uri (default: {URI_BLOCKS})
    
    Returns:
        list -- Return all informations about blocks created between two date
    """

    blocks_list = []
    blocks_list += getListBlocks_1day(start)
    start_datetime = stringToDatetime(start)
    current_dateTime = start_datetime + datetime.timedelta(days=1)
    end_dateTime = stringToDatetime(end)
    while current_dateTime <= end_dateTime:
        blocks_list += getListBlocks_1day(current_dateTime.strftime('%Y-%m-%d'))
        current_dateTime += datetime.timedelta(days=1)
    return blocks_list

def stringToDatetime(date):
    """ Convert a string date to a datetime date
    
    Arguments:
        date {string} -- Date in string format
    
    Returns:
        datetime -- Date in datetime format
    """

    timestemp = int(time.mktime(
        datetime.datetime.strptime(date, "%Y-%m-%d").timetuple()))
    return datetime.datetime.fromtimestamp(timestemp)

def filter_listBlocks(listBlocks):
    """ Filter the blocks information to keep only hash and time
    
    Arguments:
        listBlocks {list} -- All informations about blocks
    
    Returns:
        list -- Return only value, date and block id of blocks created
    """

    res = []
    for js in listBlocks['blocks']:
        currentblock = {}
        currentblock['id_block'] = js['hash']
        currentblock['time'] = js['time']
        res.append(currentblock)
    return res


def getFirstTx(block_id, host=DEFAULT_HOST, path=URI_TRANSACTIONS):
    """ Get the first transaction of a block

    Arguments:
        block_id {string} : block hash

    Keyword Arguments:
        host {string} -- API host (default: {DEFAULT_HOST})
        path {string} -- API uri (default: {URI_TRANSACTIONS})

    Returns:
        json -- first transaction in the block
    """
    res = connectionToAPI(host, path + str(block_id))
    logging.info ("Transaction : "+str(res['tx'][0]))
    return (res['tx'][0])


def send_to_consumer(start,end,producer):
    """ Send blocks created between start and end to kafka
    
    Arguments:
        start {string} -- Start date
        end {string} -- End date
        producer {KafkaProducer} -- Kafka Producer
    """

    list_blocks = getListBlocks_Ndays(start, end)
    for block in list_blocks:
        # Get the first transaction of each block
        first_tx=getFirstTx(block['id_block'])
        producer.send('miners_hist', str(first_tx).encode())
        logging.info("Sent : First transaction of block "+block['id_block'])
    producer.close()
    logging.info("End of miners addition, producer closed.")

if __name__ == "__main__":
    from config import config

    bootstrap_servers = '{0}:{1}'.format(config['kafka']['host'], config['kafka']['port'])
    producer = KafkaProducer(acks=1,max_request_size=10000000,bootstrap_servers=bootstrap_servers)
    today = datetime.date.today()
    send_to_consumer(str(today-datetime.timedelta(days=2)),str(today), producer)
    while True:
        time.sleep(86400)
        today = str(datetime.date.today())
        send_to_consumer(today,today,producer)
        logging.info("INFO")

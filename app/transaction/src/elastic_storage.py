from elasticsearch import Elasticsearch
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import DocType, Object, Integer, Date, Float, Text
from elasticsearch_dsl import Search

import json
from http import client as httpClient
from http import HTTPStatus

DEFAULT_HOST = "api.coindesk.com"
DEFAULT_URI = "/v1/bpi/currentprice/EUR.json"
DEFAULT_URI_DATE = "/v1/bpi/historical/close.json?currency=EUR"
DEFAULT_PP_INDENT = 4

class BitCoin(DocType):
    """ Defines the mapping for ElasticSearch """
    date=Date()
    value=Float()
    type=Text
    
    class Meta:
        index = 'bitcoin_price'
    
    def save(self, ** kwargs):
        return super().save(** kwargs)

def storeData(d, v, t):
    """ Store data into the ElasticSearch db """
    BitCoin.init()
    b=BitCoin(date=d,value=v,type=t)
    b.save()

def eraseData(typ, ind="bitcoin_price"):
    """ Erase data in the database by taking 2 args : type and index"""
    s = Search(index=ind).query("match", type=typ)
    response = s.delete()
    print(response)

def http_auth(elastic_conf):
    return "{0}:{1}".format(elastic_conf["username"], elastic_conf["password"])

def connectionToAPI(host, path):
    """ Connection to the API
    
    Arguments:
        host {string} -- [description]
        path {string} -- [description]
    
    Returns:
        [json] -- [File with Bitcoin value and other informations]
    """

    connection = httpClient.HTTPConnection(host)
    connection.request("GET", path)
    resp = connection.getresponse()
    result = {}
    if resp.status == HTTPStatus.OK:
        result = json.loads(resp.read().decode('utf-8'))
    connection.close()
    return result

def getCurrentPrice(host=DEFAULT_HOST, path=DEFAULT_URI):
    """ Get the current Bitcoin price 
    
    Keyword Arguments:
        host {string} -- [description] (default: {DEFAULT_HOST})
        path {string} -- [description] (default: {DEFAULT_URI})
    
    Returns:
        [json] -- [description]
    """

    return connectionToAPI(host, path)

def createCurrentDataset(jsonDataStream):
    """ Creates a list from the json data
    
    Arguments:
        jsonDataStream {json} -- [description]
    
    Returns:
        json -- [description]
    """

    currentDic = {}
    currentDic['date'] = jsonDataStream['time']['updatedISO']
    currentDic['value'] = jsonDataStream['bpi']['EUR']['rate_float']
    return currentDic

def main():
    # Defines a default Elasticsearch client
    connections.create_connection(hosts=['localhost'])

if __name__=='__main__':
    main()

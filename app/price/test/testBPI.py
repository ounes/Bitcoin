import sys, os

sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../src'))

from BitcoinPriceIndex_historical import getHistoricalPrice, createHistoricalDataset 
from elastic_storage import createCurrentDataset, getCurrentPrice

"""
To test your function, create a test function by applying wisely an assert.
"""

def testCurrentPrice():
    assert getCurrentPrice() != {}

def testDatePrice():
    start = "2018-01-04"
    end = "2018-01-04"
    assert getHistoricalPrice(start, end) != {}

def testCreateCurrentDataset():
    jsonDataStream = { 'time': { 'updatedISO': "2018-01-04" }, 'bpi': { 'EUR': { 'rate_float': 15155.2263 }}}
    assert createCurrentDataset(jsonDataStream)['date'] == "2018-01-04"
    assert createCurrentDataset(jsonDataStream)['value'] == 15155.2263

def testCreateHistoricalDataset():
    jsonData = { 'bpi': {"2018-01-04":15155.2263} }
    for val in createHistoricalDataset(jsonData):
        assert val['date'] == "2018-01-04T23:59:00"
        assert val['value'] == 15155.2263

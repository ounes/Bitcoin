# -*- coding: utf-8 -*-

import json
import re
import datetime
import time
import logging


from http import client as httpClient
from http import HTTPStatus

from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import CountVectorizer, StringIndexer, IndexToString
from pyspark.ml import Pipeline, PipelineModel
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.mllib.util import MLUtils

from elasticsearch_dsl import connections

from elastic_storage import getHistoricalPrice, createHistoricalDataset, http_auth

from config import config

access_token = '963341451273887744-fyNcKmcLd2HRYktyU3wVMshB4eYWMoh'
access_token_secret = 'fxOtX3rk3KXqiF50mFDEYTx19E3wNVMZeSIuXmozNxmHa'
consumer_key = 'bG58SBJQV8Hiqqjcu3jzXwfCL'
consumer_secret = 'kjcBffzpn9QYZsV91NZUqKhgGKBvehLyVfuvc0pm8Gh8sEPui8'

def connectionToTwitterAPI():
    listener = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return listener, auth

DEFAULT_HOST = "newsapi.org"
def getGoogleArticle(date,host=DEFAULT_HOST):
    connection = httpClient.HTTPConnection(host)
    current = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S") - datetime.timedelta(days=1)
    date_start = current.strftime("%Y-%m-%dT%H:%M:%S")
    date_end = date
    uri="/v2/everything?q=bitcoin&from="+date_start+"&to="+date_end+"&pageSize=10&apiKey=16026a4682ba43ab864daa3c3ed9658e"
    connection.request("GET", uri)
    resp = connection.getresponse()
    result = {}
    if resp.status == HTTPStatus.OK:
        result = json.loads(resp.read().decode('utf-8'))
    connection.close()
    return result

def cleaningText(tweet):
    from nltk.corpus import stopwords
    tweet_lower = tweet.lower()
    tweet_nonAlpha = re.sub('[^a-zA-Z@# ]','',tweet_lower)
    tweet_split = tweet_nonAlpha.split(' ')
    tweet_not_doublons = set(tweet_split)
    tweet_clean = []
    stop_words = set(stopwords.words('english'))
    for word in tweet_not_doublons:
        if word not in stop_words:
            tweet_clean.append(word)
    return tweet_clean

def getResponseVariables(date):
    end = stringToDatetime(date) + datetime.timedelta(days=1)
    jsonDataH = getHistoricalPrice(date[0:10],end.strftime('%Y-%m-%d'))
    historicalDataset = createHistoricalDataset(jsonDataH)
    historicalDataset_sorted = sorted(historicalDataset, key=lambda k: k['date'])
    for i in range(len(historicalDataset_sorted)-1):
        diff = historicalDataset_sorted[i+1]['value'] - historicalDataset_sorted[i]['value']
        if diff <= 0:
            if abs(diff/historicalDataset_sorted[i]['value']) <= 0.02:
                Y = 0
            elif abs(diff/historicalDataset_sorted[i]['value']) > 0.1:
                Y = 1
            elif abs(diff/historicalDataset_sorted[i]['value']) <= 0.1:
                Y = 2
        else:
            if abs(diff/historicalDataset_sorted[i]['value']) <= 0.02:
                Y = 0
            elif abs(diff/historicalDataset_sorted[i]['value']) > 0.1:
                Y = 3
            elif abs(diff/historicalDataset_sorted[i]['value']) <= 0.1:
                Y = 4
    return Y

def getCorpusPerDate(date):
    articles = getGoogleArticle(date)
    corpus = []
    date_publication = ()
    if articles:
        for art in articles['articles']:
            if art['description'] != None:
                description = art['description']
                date_publication = date_publication + (art['publishedAt'],)
                corpus = corpus + cleaningText(description)
    return corpus

def stringToDatetime(date):
    """ Convert a string date to a datetime date
    
    Arguments:
        date {string} -- Date in string format
    
    Returns:
        datetime -- Date in datetime format
    """

    return datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")

def getCorpus_between_2_dates(start,end):
    corpus = []
    corpus.append([getCorpusPerDate(start),getResponseVariables(start),start])
    current_date = stringToDatetime(start)
    end_datetime = stringToDatetime(end)
    current_date += datetime.timedelta(days=1)
    while current_date < end_datetime:
        current_date_str = current_date.strftime('%Y-%m-%dT%H:%M:%S')
        corpus.append([getCorpusPerDate(current_date_str),
                       getResponseVariables(current_date_str), current_date_str])
        current_date += datetime.timedelta(days=1)
    return corpus


def getCorpus_custom(start):
    corpus = []
    corpus.append([getCorpusPerDate(start),
                   0, start])
    return corpus

def create_model():
    vectorizer = CountVectorizer(inputCol='text', outputCol="features")

    label_indexer = StringIndexer(inputCol="label", outputCol="label_index")

    classifier = NaiveBayes(labelCol="label_index", featuresCol="features", predictionCol="label_index_predicted")

    pipeline_model = Pipeline(stages=[vectorizer, label_indexer, classifier])

    return pipeline_model

def predict_today(sc, spark, pipeline_model):
    
    today = datetime.datetime.today()
    today_str = today.strftime("%Y-%m-%dT%H:%M:%S")

    logging.info("get corpus for prediction...")
    corpus_today = getCorpus_custom(today_str)
    logging.info("get corpus for prediction done!")
    
    logging.info(str(corpus_today))

    rdd = sc.parallelize(corpus_today).map(lambda line: Row(text=line[0]))
    df = spark.createDataFrame(rdd)

    
    logging.info("transforming model for prediction...")
    predict = pipeline_model.transform(df)
    logging.info("transforming model for prediction done!")

    return predict

def add_predict(df):
    pred = df.select(df['label_index_predicted']).rdd.first()
    logging.info("prediction {0}".format(pred))
    if pred:
        pred_int = int(pred.label_index_predicted)
        logging.info(pred_int)
        if pred_int == 0:
            prediction="No change"
        elif pred_int  == 1:
            prediction="Down >10%"
        elif pred_int  == 2:
            prediction="Down <10%"
        elif pred_int  == 3:
            prediction="Up <10%"
        elif pred_int == 4:
            prediction="Up >10%"
        else:
            prediction="Unable to predict!"
        today = datetime.datetime.today()
        doc = { 'prediction': prediction }
    else:
        logging.info("empty prediction")
        doc = {'prediction': "Not available"}
    logging.info("prediction string  {0}".format(prediction))
    for i in range(24):
        doc['date'] = today.replace(hour=i).strftime("%Y-%m-%dT%H")
        connections.get_connection().index(index="bitcoin_pred", doc_type="doc", id=i, body=doc)

def main():
    import nltk
    nltk.download('stopwords')

    sc = SparkContext()
    spark = SparkSession(sc)
    datetime.datetime.now()
    corpus = getCorpus_between_2_dates("2018-01-15T23:59:00", "2018-04-07T23:59:00")

    rdd = sc.parallelize(corpus).map(lambda v: Row(text=v[0],label=v[1],date=v[2]))
    df = spark.createDataFrame(rdd)

    df.show()

    df_train, df_test = df.randomSplit([0.7,0.3])

    pipeline_model = create_model()

    model = pipeline_model.fit(df_train)
    #model.write().overwrite().save("./model")

    predict = model.transform(df_test)

    evaluator = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="label_index_predicted", metricName="accuracy")
    accuracy = evaluator.evaluate(predict)
    logging.info("Accuracy: {0}".format(str(accuracy)))

    #pipeline_model = PipelineModel.load("./model")

    logging.info("Trying to predict tomorrow trend...")
    predict = predict_today(sc,spark,model)
    logging.info("Today prediction:")
    predict.show()

    logging.info("Inserting into base")
    connections.create_connection(hosts=config['elasticsearch']['hosts'], http_auth=http_auth(config['elasticsearch']))
    add_predict(predict)

if __name__ == "__main__":
    from config import config
    while True:
        try:
            main()
        except Exception as e:
            logging.info("Unable to build model right now...")
            logging.info("Exception -> {0}".format(str(e)))
        time.sleep(21600)

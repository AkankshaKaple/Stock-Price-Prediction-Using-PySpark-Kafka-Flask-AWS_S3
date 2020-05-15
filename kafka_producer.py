# #    Spark
# from pyspark import SparkContext
# #    Spark Streaming
# from pyspark.streaming import StreamingContext
# #    Kafka
# from pyspark.streaming.kafka import KafkaUtils
# #    json parsing
# import json
#
# sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
# sc.setLogLevel("WARN")
#
# ssc = StreamingContext(sc, 5)
#
# kafkaStream = KafkaUtils.createStream(ssc, 'cdh57-01-node-01.moffatt.me:2181'
#                                       , 'spark-streaming', {'twitter':1})
#
# parsed = kafkaStream.map(lambda v: json.loads(v[1]))
#
# parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()
#
# authors_dstream = parsed.map(lambda tweet: tweet['user']['screen_name'])
#
# author_counts = authors_dstream.countByValue()
# author_counts.pprint()
#
# author_counts_sorted_dstream = author_counts.transform(\
#   (lambda foo:foo\
#    .sortBy(lambda x:( -x[1]))))
# author_counts_sorted_dstream.pprint()

# Program to create producer
from time import sleep
from json import *
import json
import requests as requests
from kafka import KafkaProducer
import requests
import os

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))
# Serializer ==> Encoding

# Reading data directly
# for e in range(1000):
#     data = {'number' : e}
#     producer.send('numtest_topic', value=data)
#     sleep(5)


# Reading data from website
time = "5min"
symbol = "MSFT"
apikey = "PRM9UE7EI1OW53RQ"
label = "Time Series ({})".format(time)
topic = "numtest_topic_1"

link = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={}&interval={}&outputsize=full&apikey={}".format(
    symbol, time, apikey)
req = requests.get(link)

data = req.text

data = data.replace('1. open', 'Open').replace('2. high', 'High').replace('3. low', 'Low').replace('4. close',
                                                                                                   'Close').replace(
    '5. volume', 'Volume')

json_obj = json.loads(data)
dict_1 = json_obj[label]

for key in dict_1.keys():
    producer.send('numtest_topic_1', value=dict_1[key])
    sleep(5)

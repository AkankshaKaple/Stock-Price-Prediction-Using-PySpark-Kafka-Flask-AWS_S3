from kafka import KafkaConsumer
from json import loads
import pickle
# from sklearn import *
import pandas as pd
import numpy as np
import os

consumer = KafkaConsumer(
    'numtest_topic_1',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    df = pd.DataFrame([message.value])
    print(df)
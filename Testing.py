from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.regression import LinearRegressionModel

sc = SparkContext()
sqlContext = SQLContext(sc)
model_1 = LinearRegressionModel.load("My_Model")
print("Model loaded successfully")

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.feature import VectorAssembler
import sys

sc= SparkContext()
sqlContext = SQLContext(sc)
house_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('file.csv')

vectorAssembler = VectorAssembler(inputCols = ['Open', "High", "Low"], outputCol = 'features')
vhouse_df = vectorAssembler.transform(house_df)
vhouse_df = vhouse_df.select(['features', 'Close'])
# print(vhouse_df.show(3))

splits = vhouse_df.randomSplit([0.7, 0.3])
# print(splits)
train_df = splits[0]
test_df = splits[1]

from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol = 'features', labelCol='Close', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))

lr_model.save("My_Model")
print("Model saved successfully")

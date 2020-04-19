from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, month
import pyspark.sql
import pandas as pd 
import numpy as np
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

logFile = "hdfs://CC-MON-25:9000/user/student/access_log"  # Should be some file on your system
spark = SparkSession.builder.appName("LinearRegressionExample").getOrCreate()

df = spark.read.option("delimiter"," ").option("header","false").csv(logFile)
df = df.select(df._c0.alias("IP"), df._c3.substr(2, 21).alias("Time"))
df = df.select("IP", "Time", to_timestamp('Time', "dd/MMM/yyyy:HH:mm:ss").alias("timestamp") )
df = df.select("IP", "Time", "timestamp", month('timestamp').alias("month") )
df.show()
gdf = df.groupBy('month').agg({'IP': 'count'})
gdf = gdf.select(gdf["count(IP)"].alias("IP_Count_month"), "month" )
gdf = gdf.where(gdf.month.isNotNull())
gdf.show()

vecAssembler = VectorAssembler(inputCols=['month'], outputCol="features")
stream_df = vecAssembler.transform(gdf)
stream_df = stream_df.select(['features', 'IP_Count_month'])
stream_df.show(3)

lr = LinearRegression(featuresCol = 'features', labelCol='IP_Count_month', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(stream_df)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))

trainingSummary = lr_model.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

lr_model.save("pythonLinearRegressionModel")

print("Program Complete")
spark.stop()
import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import Row, SparkSession
from pyspark.sql.streaming import DataStreamWriter, DataStreamReader
from pyspark.sql.functions import explode, split, window, col, count
from pyspark.sql.types import IntegerType, DateType, StringType, StructType
from pyspark.sql.functions import sum, avg, max, when
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("NasaLogSparkStreaming") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    windowDuration = '60 seconds'
    slideDuration = '30 seconds'

    # Question 1: All I did here was replace the Host and Ports with the actual given values
    logsDF = spark.readStream.format("socket").option("host", "stream-emulator.data-science-tools.svc.cluster.local")\
             .option("port", 5551).option('includeTimestamp', 'true').load()

    #Question2: time stamp given in lab 7
    logsDF = logsDF.select(explode(split(logsDF.value, " ")).alias("logs"),logsDF.timestamp)

    #changed water mark to 3 seconds (given in lab 7)
    logsDF = logsDF.withWatermark("timestamp", "3 seconds")
    
    logsDF = logsDF.withColumn('idx', split(logsDF['logs'], ',').getItem(0)) \
        .withColumn('hostname', split(logsDF['logs'], ',').getItem(1)) \
        .withColumn('time', split(logsDF['logs'], ',').getItem(2)) \
        .withColumn('method', split(logsDF['logs'], ',').getItem(3)) \
        .withColumn('resource', split(logsDF['logs'], ',').getItem(4)) \
        .withColumn('responsecode', split(logsDF['logs'], ',').getItem(5)) \
        .withColumn('bytes', split(logsDF['logs'], ',').getItem(6))

    #Question  3:
    query = logsDF.writeStream.outputMode("append")\
                       .option("numRows", "20")\
                       .option("truncate", "false")\
                       .format("console")\
                       .start()
    
    query.awaitTermination(60)
    query.stop()    

    #given in lab7
    hostCountsDF = logsDF.groupBy(window(logsDF.timestamp, windowDuration, slideDuration),logsDF.hostname).count()

    bytesreceivedDF = logsDF.groupBy(window(logsDF.timestamp, windowDuration, slideDuration), \
                                   logsDF.hostname).agg(sum('bytes').alias("total_bytes")).na.drop(subset=["hostname", "total_bytes"])
    
    responseStatusDF = logsDF.filter((logsDF.method == "GET") & (logsDF.responsecode == "200")) \
        .groupBy(logsDF.hostname.alias('window')) \
        .count().withColumnRenamed("count", "gif_count")
    
    gifCountDF = logsDF \
                   .filter(col("resource").contains("gif")) \
                   .groupBy(window(logsDF.timestamp, windowDuration, slideDuration)) \
                   .count() \
                   .withColumnRenamed("count", "Correct_count")

    query = gifCountDF.writeStream.outputMode("update")\
                       .option("numRows", "100000")\
                       .option("truncate", "false")\
                       .format("console")\
                       .start()    
    
    query = responseStatusDF.writeStream.outputMode("complete")\
                       .option("numRows", "100000")\
                       .option("truncate", "false")\
                       .format("console")\
                       .trigger(processingTime='10 seconds') \
                       .start()    
    
    
    query.awaitTermination(100)
    query.stop()    


    spark.stop()


import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

from functools import reduce
from pyspark.sql.functions import col, lit, when, concat_ws
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
import graphframes
from graphframes import *


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12")\
        .appName("graphframes")\
        .getOrCreate()

    sqlContext = SQLContext(spark)
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    taxi_zone_lookup_path = "s3a://data-repository-bkt/ECS765/nyc_taxi/taxi_zone_lookup.csv"
    df_taxi_zone_lookup = spark.read.option("header", "true").csv(taxi_zone_lookup_path)

    green_taxi_data_path = "s3a://data-repository-bkt/ECS765/nyc_taxi/green_tripdata/2023/*.csv"
    df_green_taxi = spark.read.option("header", "true").csv(green_taxi_data_path)

    #Question 1: total number of entries
    total_entries = df_green_taxi.count()
    print(f"The number of entries is: {total_entries}")

    #Question 2: Set up the StructTypes for vertex schema and edges schema    vertexSchema = StructType([
    vertexSchema = StructType([
        StructField("LocationID", IntegerType(), True),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True)
    ])

    edgeSchema = StructType([
        StructField("src", IntegerType(), False),
        StructField("dst", IntegerType(), False)
    ])

    #Set up the Data frames for the edges and vertices
    edgesDF = spark.read.format("csv").options(header='True').csv(green_taxi_data_path).select(col("PULocationID").alias("src"), col("DOLocationID").alias("dst"))
    verticesDF = spark.read.format("csv").options(header='True').schema(vertexSchema).csv(taxi_zone_lookup_path)

    edgesDF.show(5, truncate=False)
    verticesDF.show(5, truncate=False)

    #Question 3: show the graphframe requested
    graph.triplets.show(truncate=False)

    #Question 4: search for structural pattern through connected vertices
    same_borough_and_zone = graph.find("(a)-[]->(b)").filter("a.Borough = b.Borough and a.service_zone = b.service_zone")
    print("count: %d" % same_borough_and_zone.count())

    same_borough_and_zone.select("a.id", "b.id", "a.Borough", "a.service_zone").show(10)

    #Question 5: atarget vertex 1
    target_vertices = [1] 
    #find the shortest path to target vertex
    shortest_paths_df = graph.shortestPaths(landmarks=target_vertices)
    #create the DataFrame as requested
    shortest_paths_df = shortest_paths_df.withColumn(
        "id_to_1", concat_ws("->", shortest_paths_df["id"], lit(1))
    ).withColumn(
        "shortest_distance", shortest_paths_df["distances"][1]
    )
    
    shortest_paths_df.select("id_to_1", "shortest_distance").show(10, truncate=False)

    #Question 6: creating a page rank:
    page_rank = graph.pageRank(resetProbability=0.17, tol=0.01).vertices.sort('pagerank', ascending=False)
    page_rank.select("id", "pagerank").show(5, truncate=False)


    spark.stop()
import sys, string
import os
import math
import socket
from pyspark.sql import SparkSession
from datetime import datetime

from pyspark.sql.functions import from_unixtime, date_format, col, to_date, concat_ws, sum, month, to_timestamp, count, \
    year, countDistinct, expr, round, unix_timestamp, udf
from pyspark.sql.types import FloatType, IntegerType, DoubleType

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("task2") \
        .getOrCreate()
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")


    eth_blocks_path = "s3a://data-repository-bkt/ECS765/ethereum/blocks.csv"
    df_eth_blocks = spark.read.option("header", "true").csv(eth_blocks_path)
#   df_eth_blocks.show()


    eth_transactions_path = "s3a://data-repository-bkt/ECS765/ethereum/transactions.csv"
    df_eth_transactions = spark.read.option("header", "true").csv(eth_transactions_path)
#   df_eth_transactions.show()
    
    #Quesation 1: print 2 Schemas for blocks and transactions
    df_eth_blocks.printSchema()
    df_eth_transactions.printSchema()

    #Question 2: I selected the miners and the sizes and ordered them in descending order. (also renamed size -> total_size)
    df_miner_by_size = df_eth_blocks.select("miner", "size").withColumnRenamed("size", "total_size").orderBy(col("size").desc())
    df_miner_by_size.show(10)

    #Question 3: selected the timestamp (not formatted) and apply a from_unixtime function to format it to yyy-MM-dd format
    formatted_date = df_eth_blocks.select(
        "timestamp"
    ).withColumn(
        "formatted_date",
        date_format(from_unixtime("timestamp"), "yyyy-MM-dd")
    )

    formatted_date.show(10)

    #Question 4: Performed a join of both DFs above and counted it;s entries (no. of lines)
    joined_result_df = (
        df_eth_transactions.join(
            df_eth_blocks,
            df_eth_blocks.hash == df_eth_transactions.block_hash,
            "inner"
        )
    )

    total_entries = joined_result_df.count()
    print(f"The number of lines is: {total_entries}")

    #Question 5 (Incomplete): filtered the DF to only show month of september
    final_result_df = (
        joined_result_df.join(
            formatted_date_df,
            formatted_date_df.timestamp == joined_result_df.timestamp,
            "inner"
        )
    )
    
    filtered_df = final_result_df.filter(
        (col("formatted_date") >= "2015-09-01") & (col("formatted_date") < "2015-10-01")
    )
    #grouped by formatted date and aggregated the block_number and from address address
    grouped_df = filtered_df.groupBy(col("formatted_date")).agg(
        countDistinct("block_number").alias("block_count"),
        countDistinct("from_address").alias("unique_senders_count_number")
    )
    #Ordered by formatted_date
    sorted_df = grouped_df.orderBy(col("formatted_date"))

    sorted_df.show(10, truncate=False)

    #Question 6 (Incomplete, probably with redundancy):
    joined_result_df = (
        df_eth_transactions.join(
            df_eth_blocks,
            df_eth_blocks.hash == df_eth_transactions.block_hash,
            "inner"
        )
    )

    final_result_df = (
        joined_result_df.join(
            formatted_date_df,
            formatted_date_df.timestamp == joined_result_df.timestamp,
            "inner"
        )
    )
    #filter for only the month of october and where transaction index = 0
    filtered_df = final_result_df.filter(
        (col("formatted_date") >= "2015-10-01") & (col("formatted_date") < "2015-11-01")
    ).filter(
        (col("transaction_index") == 0)
    )
    #add column for the total gass fee price
    filtered_df = filtered_df.withColumn("total_fee", col("gas") * col("gas_price"))
    #the rest is self evidant, group by formatted date, sum the total_fees
    grouped_df = filtered_df.groupBy(col("formatted_date")).agg(
        sum("total_fee").alias("total_transaction_fee")
    )
    #put in order
    sorted_df = grouped_df.orderBy(col("formatted_date"))
    #select only those 2 columns (as shown in the question)
    final_df = sorted_df.select("formatted_date", "total_transaction_fee")
    
    sorted_df.show()
    


#    now = datetime.now()
#    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
#    rideshare_tripZone_df.coalesce(1).write.csv("s3a://" + s3_bucket + "/merged_data_" + date_time + ".csv", header=True)
#    rideshare_tripZone_df.write.csv("s3a://" + s3_bucket + "/processed_data_" + date_time, header=True)
    

    
    spark.stop()

import sys, string
import os
import socket
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format, col, to_date, concat_ws, sum, month, to_timestamp, count
from pyspark.sql.types import FloatType, IntegerType


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("task1")\
        .getOrCreate()
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

    #Set up the paths, se up the DataFrames, show the Schamas and print them (not asked for)
    taxi_zone_lookup_path = "s3a://data-repository-bkt/ECS765/nyc_taxi/taxi_zone_lookup.csv"
    df_taxi_zone_lookup = spark.read.option("header", "true").csv(taxi_zone_lookup_path)
    df_taxi_zone_lookup.show()
    df_taxi_zone_lookup.printSchema()

    yellow_taxi_data_path = "s3a://data-repository-bkt/ECS765/nyc_taxi/yellow_tripdata/2023/*.csv"
    df_yellow_taxi = spark.read.option("header", "true").csv(yellow_taxi_data_path)
    df_yellow_taxi.show()
    df_yellow_taxi.printSchema()

    #Question1: Simply prints the number of rows in the dataframe (aka entries)
    total_entries = df_yellow_taxi.count()
    print(f"The number of entries is: {total_entries}")

    #Question2:
    
    #these are for debugging and were used as 'rough' work to test functionality
    #high_fare_count = df_yellow_taxi.filter(df_yellow_taxi.fare_amount > 50).count()
    #print(f"Number of entries with fare_amount > 50: {high_fare_count}")

    #filter the yellow taxi DF so that fare amount is larger than 50, trip distance less than 10 and only in a week of march
    filter_df = df_yellow_taxi.filter(
        (col("fare_amount") > 50) & 
        (col("trip_distance") < 1) & 
        (col("tpep_pickup_datetime").between("2023-02-01", "2023-02-07"))
    )

    #This was used to group by PU date and to change it's format (not including time)
    result_df = filter_df.withColumn(
        "pickup_date", date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd")
    ).groupBy("pickup_date").count()
    
    result_df.show(truncate=False)

    #Question 3: Sorry for any redundancy this question took me a while to understand, couldn't wrap my head around it

    #Perform an inner join by joining the Pick up Location ID with the Location ID in the yellow taxi and zone look up
    #then I selected only the columns Borough, pickup zone, zone & service zone and renamed them
    result_df = (
        df_yellow_taxi.join(
            df_taxi_zone_lookup.alias("pickup_zone"),
            df_yellow_taxi.PULocationID == df_taxi_zone_lookup.alias("pickup_zone").LocationID,
            "inner"
        )
        .select(
            df_yellow_taxi["*"],
            col("pickup_zone.Borough").alias("Pickup_Borough"),
            col("pickup_zone.Zone").alias("Pickup_zone"),
            col("pickup_zone.service_zone").alias("Pickup_service_zone"),
        )
    )

    #dropped these as asked by the question
    result_df = result_df.drop("LocationID", "PULocationID")

    #I performed the same operations as above by for the drop_off zones and renamed accordingly
    new_result_df = (
        result_df.join(
            df_taxi_zone_lookup.alias("dropoff_zone"),
            df_yellow_taxi.DOLocationID == df_taxi_zone_lookup.alias("dropoff_zone").LocationID,
            "inner"
        )
        .select(
            result_df["*"],
            col("dropoff_zone.Borough").alias("Dropoff_Borough"),
            col("dropoff_zone.Zone").alias("Dropoff_zone"),
            col("dropoff_zone.service_zone").alias("Dropoff_service_zone"),
        )
    )

    #Dropoped these again as asked by the question
    new_result_df = new_result_df.drop("LocationID", "DOLocationID")
    new_result_df.printSchema()

    #Question 4: added 2 new columns, route which is a concatanation of PU Borough to DO Borough, and Month, which picks the month from tthe PU dateTime
    final_result_df = new_result_df.withColumn(
        "route",
        concat_ws(" to ", col("Pickup_Borough"), col("Dropoff_Borough"))
    ).withColumn(
        "Month",
        month("tpep_pickup_datetime")
    )

    final_result_df.printSchema()
    final_result_df.show(10, truncate=False)

    #Question 5: I aggregated the count of all the passangers and added their total tip. I also averaged the tips by doing col("total_tip_amount")/col("passenger_count")

    final_result_df = final_result_df.groupBy("Month", "route").agg(
        count("*").alias("passenger_count"),
        sum("tip_amount").alias("total_tip_amount")
    )
    final_result_df = final_result_df.withColumn(
        "average_tip_per_passenger",
        (col("total_tip_amount")/col("passenger_count"))
    )

    
    final_result_df.printSchema()
    final_result_df.show(10, truncate=False)  

    #Question 6 & 7 : filtered the above DF to only show where avg tip == 0, the for Q7 ordered the Routes by ordering the avg tip

    zero_avg_tip_df = final_result_df.filter(col("average_tip_per_passenger") == 0)
    top_routes_df = final_result_df.orderBy(col("average_tip_per_passenger").desc())
    
    
    final_result_df.printSchema()
    zero_avg_tip_df.show(10, truncate=False)
    top_routes_df.show(10, truncate=False)

    spark.stop()

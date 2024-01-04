from confluent_kafka import Consumer
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType 
from pyspark.sql.functions import when

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaConsumerToCassandra") \
    .config("spark.cassandra.connection.host", "cass_cluster") \
    .getOrCreate()

# Define schema for the incoming Kafka message
schema = StructType() \
    .add("userid", StringType()) \
    .add("user_location", StringType()) \
    .add("channelid", IntegerType()) \
    .add("genre", StringType()) \
    .add("lastactive", StringType()) \
    .add("title", StringType()) \
    .add("watchfrequency", IntegerType()) \
    .add("etags", StringType())


# Read data from Kafka topic
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker") \
    .option("subscribe", "streamTopic") \
    .load()

# Convert the value column from Kafka into string and parse JSON
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Perform necessary transformations on the parsed data 
# ETL to augment with 'impression' column
netflix_df = parsed_df.withColumn('impression',
    when(parsed_df['watchfrequency'] < 3, "neutral")
    .when(((parsed_df['watchfrequency'] >= 3) & (parsed_df['watchfrequency'] <= 10)), "like")
    .otherwise("favorite")
)
# Drop the etags values
netflix_transformed_df = netflix_df.drop('etags')

# Write data to Cassandra
netflix_transformed_df.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "netflixdata") \
    .option("table", "netflix_data") \
    .start() \
    .awaitTermination()

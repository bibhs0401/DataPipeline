from confluent_kafka import Consumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, when
from pyspark.sql.types import StructType, StringType, IntegerType

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic = 'streamTopic'
group_id = 'group_id'

conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'
}

# Create Kafka consumer
consumer = Consumer(conf)
consumer.subscribe([topic])
access_key = "your access key"
secret_access_key = "your secret access key"

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaConsumerToS3") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_access_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "True") \
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

# Read Kafka messages in Spark Streaming
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", topic) \
    .load()

# Convert the value column from Kafka into string and parse JSON
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Perform necessary transformations on the parsed data 
netflix_df = parsed_df.withColumn('impression',
    when(parsed_df['watchfrequency'] < 3, "neutral")
    .when(((parsed_df['watchfrequency'] >= 3) & (parsed_df['watchfrequency'] <= 10)), "like")
    .otherwise("favorite")
)

# Drop the etags values
netflix_transformed_df = netflix_df.drop('etags')


output_path = "s3a://bibhusha-demo-bucket/streamData/"
query = netflix_transformed_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://bibhusha-demo-bucket/checkpoint/") \
    .start(output_path)

# Await termination
query.awaitTermination()

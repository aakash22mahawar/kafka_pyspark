from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AakashMahawar\anaconda3\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AakashMahawar\anaconda3\python.exe'

class KafkaConsumer:
    def __init__(self, bootstrap_servers, topic):
        self.spark = SparkSession.builder \
        .appName("KafkaConsumer") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0') \
        .config("spark.sql.shuffle.partitions", 4)\
        .getOrCreate()
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

    def create_schema(self):
        """
        Creates the schema for the DataFrame.
        """
        return StructType([
            StructField("name", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("country", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True)
        ])

    def consume(self):
        """
        Reads streaming data from Kafka and processes it.
        """
        # Define schema
        json_schema = self.create_schema()

        # Subscribe to Kafka topic
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "latest")\
            .load()

        # Parse value from binay to string
        json_df = df.selectExpr("cast(value as string) as value")

        json_df = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*")


        # Start query
        query = json_df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

        # Wait for termination
        query.awaitTermination()

if __name__ == "__main__":
    kafka_consumer = KafkaConsumer(bootstrap_servers="localhost:9092", topic="random_name")
    kafka_consumer.consume()

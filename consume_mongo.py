from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AakashMahawar\anaconda3\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AakashMahawar\anaconda3\python.exe'

class MongoConsumer:
    def __init__(self, bootstrap_servers, topic, mongodb_uri, mongodb_database, mongodb_collection):
        self.spark = SparkSession.builder \
            .appName("KafkaConsumer") \
            .config("spark.streaming.stopGracefullyOnShutdown", True) \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .config("spark.sql.shuffle.partitions", 4) \
            .config("spark.mongodb.output.uri", mongodb_uri) \
            .getOrCreate()
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.mongodb_database = mongodb_database
        self.mongodb_collection = mongodb_collection

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
            .option("startingOffsets", "latest") \
            .load()

        # Parse value from binay to string
        json_df = df.selectExpr("cast(value as string) as value")

        json_df = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*")

        # Start queries
        ## Write data to console ##

        query_1 = json_df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

        # Write data to MongoDB
        query_2 = json_df \
            .writeStream \
            .outputMode("append") \
            .foreachBatch(self.write_to_mongodb) \
            .start()

        # Wait for termination
        query_1.awaitTermination()
        # Wait for termination
        query_2.awaitTermination()

    def write_to_mongodb(self, batch_df, batch_id):
        """
        Writes a batch DataFrame to MongoDB.
        """
        batch_df.write \
            .format("mongo") \
            .mode("append") \
            .option("database", self.mongodb_database) \
            .option("collection", self.mongodb_collection) \
            .save()

        print(f"++++++++++++++ item has been pushed into MongoDB +++++++++++++++")

if __name__ == "__main__":
    mongo_consumer = MongoConsumer(
        bootstrap_servers="localhost:9092",
        topic="random_name",
        mongodb_uri="mongodb+srv://myAtlasDBUser:PASSWORD@myatlasclusteredu.zxqa7ir.mongodb.net",
        mongodb_database="kafka_pyspark_stream",
        mongodb_collection="random_name"
    )
    mongo_consumer.consume()

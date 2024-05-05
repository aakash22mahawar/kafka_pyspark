from pyspark.sql import SparkSession

# Set the environment variables for PySpark to use Anaconda Python
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AakashMahawar\anaconda3\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AakashMahawar\anaconda3\python.exe'

# Create a SparkSession
spark = SparkSession.builder.appName('test').config('spark.local.dir', r"C:\spark\temp").getOrCreate()
# Set the log level
spark.sparkContext.setLogLevel("ERROR")

# Read the CSV file into a DataFrame
df = spark.read.csv(r"C:\Users\AakashMahawar\Downloads\rumah_counts.csv", sep=",", header=True, inferSchema=True)

# Display the first few rows of the DataFrame
df.show(5)
spark.stop()

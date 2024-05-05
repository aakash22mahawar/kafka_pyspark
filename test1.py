from pyspark.sql import SparkSession
import pandas as pd

import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AakashMahawar\anaconda3\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AakashMahawar\anaconda3\python.exe'

spark = SparkSession.builder.appName("DataFarme").getOrCreate()
df = spark.createDataFrame([("Java", "20000"), ("Python", "100000"), ("Scala", "3000"),("R","90000")])
df.show()
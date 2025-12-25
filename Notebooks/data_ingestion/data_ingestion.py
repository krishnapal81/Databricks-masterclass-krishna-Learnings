# Databricks notebook source
# Notebook: data_ingestion

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Example input (you can replace with autoloader later)
data = [
    (1, "Laptop", 50000),
    (2, "Mouse", 800),
    (3, "Keyboard", 1200)
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("price", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)

# Write to Bronze Delta table
bronze_path = "/mnt/bronze/products"

df.write.format("delta").mode("overwrite").save(bronze_path)

print("Notebook 1 Completed: Bronze data written successfully")

# Databricks notebook source
# MAGIC %md Basics of DataBricks

# COMMAND ----------

data=[(1,"krishna",22,30),(2,"pal",25,40)]
schema="id INT,name STRING,age INT,marks INT"
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Some Important Interview Questions based on Basics [](url)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is Databricks
# MAGIC Databricks is a unified data platform built on Apache Spark for scalable data engineering and analytics.****

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is a DataFrame
# MAGIC A DataFrame is a distributed collection of data organized into rows and columns, similar to a table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### show() vs display()
# MAGIC show() is a Spark action, while display() is a Databricks visualization helper

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lazy Evaluation
# MAGIC - Spark does not execute immediately
# MAGIC - Execution happens only when an action is called
# MAGIC ## Actions examples:
# MAGIC - show()
# MAGIC - count()
# MAGIC - write()
# MAGIC ###  Spark uses lazy evaluation, meaning transformations are executed only when an action is triggered.

# COMMAND ----------

# MAGIC %md
# MAGIC # Where Data Lives
# MAGIC data is stored in DBFS or cloud storage.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why Databricks is Fast
# MAGIC - Parallel processing
# MAGIC - In-memory computation
# MAGIC - Optimized Spark engine

# COMMAND ----------


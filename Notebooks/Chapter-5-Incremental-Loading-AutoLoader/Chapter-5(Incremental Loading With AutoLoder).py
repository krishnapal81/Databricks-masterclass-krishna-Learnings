# Databricks notebook source
# MAGIC %md
# MAGIC # Auto Loader

# COMMAND ----------

# MAGIC %md
# MAGIC **we need to create Streaming DataFrame**

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://autoloderdestination@datalakekrishnaone.dfs.core.windows.net/checkpoint") \
    .load("abfss://autolodersource@datalakekrishnaone.dfs.core.windows.net")


# COMMAND ----------

df.writeStream.format("delta") \
    .option("checkpointLocation",
        "abfss://autoloderdestination@datalakekrishnaone.dfs.core.windows.net/checkpoint/write") \
    .start("abfss://autoloderdestination@datalakekrishnaone.dfs.core.windows.net/data")


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE STORAGE CREDENTIAL databrickamaster;
# MAGIC DESCRIBE STORAGE CREDENTIAL dl_cred;
# MAGIC DESCRIBE STORAGE CREDENTIAL dl_cred1;
# MAGIC

# COMMAND ----------


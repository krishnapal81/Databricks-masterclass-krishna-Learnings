# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

dbutils.secrets.list(scope='krishnascope')

# COMMAND ----------

dbutils.secrets.get(scope='krishnascope',key='app-secret')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.datalakekrishnaone.dfs.core.windows.net",
    dbutils.secrets.get(
        scope="krishnascope",
        key="app-secret"
    )
)

# COMMAND ----------

df_sales=(spark.read.format("csv")
        .option("header",True)
        .option("inferSchema",True)
        .load('abfss://source@datalakekrishnaone.dfs.core.windows.net/')
)


# COMMAND ----------

display(df_sales)


# COMMAND ----------

# MAGIC %md
# MAGIC ## PySpark Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformation 1**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_sales.withColumn('Item_Type',split(col('Item_Type'),' ')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformation 2**

# COMMAND ----------

dbutils.widgets.text('Dynamic','krishna')

# COMMAND ----------

var=dbutils.widgets.get('Dynamic')

# COMMAND ----------

var


# COMMAND ----------

df_sales.withColumn('Flag',lit(var)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **"In the above examples we implementes the pySpark transformation and in second we use Widgets function to add the dynamic value as a new cloumn called the flag variables"**

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformation 3** Changing the data type for a column 

# COMMAND ----------

df_sales.withColumn('Item_Visibility',col("Item_Visibility").cast(StringType())).display()

# COMMAND ----------


# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC **Run Utiltity**
# MAGIC - allows you to execute one notebook from another
# MAGIC -no need to create another data frames which are alredy created in previous notebooks 
# MAGIC -so we can run previous notebook in new note book by following a simple command or by using run utility

# COMMAND ----------

# MAGIC %run "/DataBricksMasterClass/Chapter-3(Data Reading)"

# COMMAND ----------

# MAGIC %md
# MAGIC **With the below command data is transfer from source block to destnation block with both deltalogs file and and parquet file **

# COMMAND ----------

df_sales.write.format("delta")\
                .mode('append')\
                .option('path','abfss://destination@datalakekrishnaone.dfs.core.windows.net/sales')\
                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Managed vs External Delta Tables 

# COMMAND ----------

# MAGIC %md
# MAGIC **DataBase**

# COMMAND ----------

# MAGIC %sql
# MAGIC create database sales_db

# COMMAND ----------

# MAGIC %md
# MAGIC **Managed dalta tables**

# COMMAND ----------

# MAGIC %sql
# MAGIC create table sales_db.managedTable
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   marks INT
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into sales_db.managedTable
# MAGIC values
# MAGIC (1,'John',80),(2,'Bob',90),(3,'Ali',70),(4,'Kate',60),(5,'Mary',50)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_db.managedtable

# COMMAND ----------

# MAGIC %md
# MAGIC if i delete the managedtable then data will delete but in case of external table data will not be delte lets see in next example ,data recides in the cloud account it will delte from the data bricks catalog , when we run drop table query

# COMMAND ----------

# MAGIC %md
# MAGIC ## External Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sales_db.externalTable
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   marks INT
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION 'abfss://destination@datalakekrishnaone.dfs.core.windows.net/sales_db/externalTable';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into sales_db.externalTable
# MAGIC values
# MAGIC (1,'John',80),(2,'Bob',90),(3,'Ali',70),(4,'Kate',60),(5,'Mary',50)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_db.externalTable

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Managed Table vs External Delta Table – Notes
# MAGIC 🔷 1. Definition
# MAGIC Managed Table
# MAGIC
# MAGIC Databricks manages both:
# MAGIC
# MAGIC metadata (schema, table definition)
# MAGIC
# MAGIC data files on storage
# MAGIC
# MAGIC External Table (Delta)
# MAGIC
# MAGIC Databricks manages only:
# MAGIC
# MAGIC metadata
# MAGIC
# MAGIC Your cloud storage controls:
# MAGIC
# MAGIC data location and lifecycle
# MAGIC
# MAGIC 🔷 2. Storage Location
# MAGIC Managed:
# MAGIC
# MAGIC Databricks stores data under workspace-managed storage path
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC /Volumes/<catalog>/<schema>/<table>/
# MAGIC
# MAGIC
# MAGIC ✔ You don’t specify location during CREATE TABLE
# MAGIC
# MAGIC External:
# MAGIC
# MAGIC You MUST specify cloud location
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC LOCATION 'abfss://container@storage.dfs.core.windows.net/folder/table'

# COMMAND ----------

# MAGIC %md
# MAGIC # . Who controls data lifecycle?
# MAGIC | Table Type | Who owns the physical files? |
# MAGIC | ---------- | ---------------------------- |
# MAGIC | Managed    | Databricks (Unity Catalog)   |
# MAGIC | External   | You / Cloud storage admin    |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # DROP TABLE behavior
# MAGIC | Operation  | Managed Table            | External Table        |
# MAGIC | ---------- | ------------------------ | --------------------- |
# MAGIC | DROP TABLE | Deletes metadata + files | Deletes metadata ONLY |
# MAGIC | VACUUM     | Allowed                  | Allowed               |
# MAGIC
# MAGIC
# MAGIC ## External table DROP does NOT delete files

# COMMAND ----------

# MAGIC %md
# MAGIC # When to use which?
# MAGIC | Use Case                                 | Recommended |
# MAGIC | ---------------------------------------- | ----------- |
# MAGIC | You want UC full governance + simplicity | Managed     |
# MAGIC | Data shared across tools/platforms       | External    |
# MAGIC | You want to manage raw data manually     | External    |
# MAGIC | Temporary staging tables                 | Managed     |
# MAGIC | Data migrations between clouds           | External    |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Important internal difference
# MAGIC | Feature                                   | Managed Table | External Table |
# MAGIC | ----------------------------------------- | ------------- | -------------- |
# MAGIC | storage path automatically created        | ✔             | ❌              |
# MAGIC | path must pre-exist                       | ❌             | ✔              |
# MAGIC | parent registered as UC external location | ❌             | ✔ required     |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Interview one-liner
# MAGIC Managed tables store both metadata and data under Databricks-managed storage. External tables store data at a user-controlled cloud path, and UC manages only metadata. External tables require external locations and RBAC IAM permissions, and DROP TABLE won’t delete data files.

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Table Functionalities

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into sales_db.externalTable
# MAGIC values
# MAGIC (6,'John',90),(7,'Bob',96),(8,'Ali',78),(9,'Kate',90),(10,'Mary',90)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_db.externaltable

# COMMAND ----------

# MAGIC %md
# MAGIC everytime when new data is inserted in the external table a new parquet file is cleated automaatically in tour cloud storage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from sales_db.externaltable where id=10

# COMMAND ----------

# MAGIC %md
# MAGIC # Versioning

# COMMAND ----------

# MAGIC %md
# MAGIC ## Versioning means every write operation creates a new version of a Delta table
# MAGIC - Stored in _delta_log folder
# MAGIC - Enables audit + reproducibility
# MAGIC - Uses MVCC for safe concurrent reads/writes
# MAGIC - Each commit recorded as metadata + file changes
# MAGIC - Version numbers increase sequentially
# MAGIC - Retention period controls how long old versions stay

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history sales_db.externalTable
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Time Travel

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time travel allows querying or restoring the table to a previous version or timestamp.
# MAGIC - Access old snapshots safely
# MAGIC - Supports both version and timestamp filters
# MAGIC - Useful for debugging, validation, rollback
# MAGIC - Works only while versions are retained
# MAGIC - VACUUM permanently deletes old snapshot files

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table sales_db.externalTable to version as of 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_db.externalTable

# COMMAND ----------

# MAGIC %md
# MAGIC # Vaccum Command 

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM removes old, unreferenced data files from a Delta table to free storage and maintain performance.
# MAGIC - Deletes files older than retention threshold
# MAGIC - Default retention = 7 days
# MAGIC - Helps control storage costs
# MAGIC - Improves metadata management
# MAGIC - After VACUUM, old versions may be unrecoverable (no time travel past retention
# MAGIC - Use carefully, especially in production
# MAGIC - Can set custom retention window

# COMMAND ----------

# MAGIC %md
# MAGIC ## this command will delete the files if file is older than 7 Days otherwise it will do nothing 

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum sales_db.externalTable;

# COMMAND ----------

# MAGIC %md
# MAGIC ## this command will detelt immeditially due to retain it will not check anything means how older is the file 

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum sales_db.externalTable retain 0 hours

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Table Optimization 

# COMMAND ----------

# MAGIC %md
# MAGIC **Otimize command**
# MAGIC differences must be seen into large table we are using small tables lets see the result with normal query and optimise query in below ,oberrve that optimize command is taken 2.31 sec and normal command is taken 2.42 seconds ,if we use large data sets then we will able to see differencr around 30-40 percent 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_db.externalTable

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize sales_db.externalTable

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from sales_db.externaltable

# COMMAND ----------

# MAGIC %md
# MAGIC # ZORDER BY Command 

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize sales_db.externalTable zorder by(id);
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_db.externalTable

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://autoloderdestination@datalakekrishnaone.dfs.core.windows.net/checkpoint") \
    .load("abfss://autolodersource@datalakekrishnaone.dfs.core.windows.net")


# COMMAND ----------

df.writeStream.format("delta") \
    .option("checkpointLocation", "abfss://autoloderdestination@datalakekrishnaone.dfs.core.windows.net/checkpoint") \
    .start("abfss://autoloderdestination@datalakekrishnaone.dfs.core.windows.net/data")

# COMMAND ----------


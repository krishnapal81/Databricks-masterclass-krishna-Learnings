# Databricks notebook source
# MAGIC %md
# MAGIC > ### DBFS(Databricks File System)
# MAGIC > 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Access Data

# COMMAND ----------

app_id="b98ba86a-8a53-45e8-ae65-5fe4becb7fae"
tanent_id="ad7cdf1f-91d8-4574-9280-89d4d999da23"
secret="j7O8Q~XrlqoPW4IXrMWYvD1U0crtW0YctSa2icem"

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.auth.type.datalakekrishnaone.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    "fs.azure.account.oauth.provider.type.datalakekrishnaone.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(
    "fs.azure.account.oauth2.client.id.datalakekrishnaone.dfs.core.windows.net",
    "b98ba86a-8a53-45e8-ae65-5fe4becb7fae"
)
spark.conf.set(
    "fs.azure.account.oauth2.client.secret.datalakekrishnaone.dfs.core.windows.net",
    "j7O8Q~XrlqoPW4IXrMWYvD1U0crtW0YctSa2icem"
)
spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint.datalakekrishnaone.dfs.core.windows.net",
    "https://login.microsoftonline.com/ad7cdf1f-91d8-4574-9280-89d4d999da23/oauth2/token"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # DataBricks Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC **1.dbutils.fs()**

# COMMAND ----------

# MAGIC %md
# MAGIC ## this synatax is very important to check the connectivity of source and also check the files path too
# MAGIC dbutils.fs.ls("abfss://soucecontainername@datalakekrishnaone.dfs.core.windows.net/")

# COMMAND ----------

dbutils.fs.ls("abfss://source@datalakekrishnaone.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC **2. dbutils.widgets()**

# COMMAND ----------

# MAGIC %md
# MAGIC Wedgets utility allows you to parameterize notebook
# MAGIC there are many types we will see one by one or we can also see by dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.secrets.list(scope='krishnascope')

# COMMAND ----------

dbutils.secrets.get(scope='krishnascope',key='app-secret')

# COMMAND ----------

dbutils.widgets.combobox(name="Combobox",defaultValue="Apple",choices=["apple","banana","orange"],label="Friuits")

# COMMAND ----------

app_secret = dbutils.secrets.get(
    scope="krishnascope",
    key="app-secret"
)

# COMMAND ----------

dbutils.widgets.dropdown(name="Dropdown",defaultValue="Apple",choices=["Apple","banana","orange"],label="fruits")


# COMMAND ----------

dbutils.widgets.multiselect(name="Multiselect",defaultValue="Apple",choices=["Apple","banana","orange"],label="fruits")

# COMMAND ----------

dbutils.widgets.text(name="FruitChoices",defaultValue="Apple",label="TextBox")


# COMMAND ----------

dbutils.widgets.get("Multiselect")
"with the help of name attribute we can get the value of the widget"


# COMMAND ----------

# MAGIC %md
# MAGIC **3. dbutils.secrets()**

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC 1. DBFS (Databricks File System)
# MAGIC
# MAGIC DBFS = Databricks virtual filesystem
# MAGIC
# MAGIC Provides unified access to cloud storage (S3 / ADLS / GCS)
# MAGIC
# MAGIC DBFS itself does not store data
# MAGIC
# MAGIC Spark can read/write from DBFS paths directly
# MAGIC
# MAGIC Why DBFS exists
# MAGIC
# MAGIC Simplifies cloud paths
# MAGIC
# MAGIC Enables Spark to access files like a normal file system
# MAGIC
# MAGIC 2. DBFS Path Types
# MAGIC Path	Meaning	Used In
# MAGIC dbfs:/	Spark filesystem path	Spark APIs
# MAGIC /dbfs/	Local mount path	Local ops / python file I/O
# MAGIC 3. DBFS Mounts
# MAGIC
# MAGIC Mount = link cloud storage → DBFS path
# MAGIC
# MAGIC Example mount point: /mnt/raw
# MAGIC
# MAGIC Benefits
# MAGIC
# MAGIC Shorter paths
# MAGIC
# MAGIC Easier permissions
# MAGIC
# MAGIC Consistent across notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Complete List of Databricks Utilities
# MAGIC 1. dbutils.fs – File system utilities
# MAGIC
# MAGIC Used to interact with DBFS + cloud storage.
# MAGIC
# MAGIC Common commands:
# MAGIC
# MAGIC ls
# MAGIC
# MAGIC cp
# MAGIC
# MAGIC mv
# MAGIC
# MAGIC rm
# MAGIC
# MAGIC mkdirs
# MAGIC
# MAGIC put
# MAGIC
# MAGIC 2. dbutils.notebook – Notebook workflow utilities
# MAGIC
# MAGIC Used when notebooks call each other.
# MAGIC
# MAGIC Functions:
# MAGIC
# MAGIC run() – run another notebook
# MAGIC
# MAGIC exit() – stop notebook execution and return value
# MAGIC
# MAGIC 3. dbutils.secrets – Secure credential utilities
# MAGIC
# MAGIC Retrieve credentials from secret scopes.
# MAGIC
# MAGIC Functions:
# MAGIC
# MAGIC get(scope, key)
# MAGIC
# MAGIC list(scope)
# MAGIC
# MAGIC listScopes()
# MAGIC
# MAGIC 4. dbutils.widgets – Notebook parameterization utilities
# MAGIC
# MAGIC Create input widgets like:
# MAGIC
# MAGIC text
# MAGIC
# MAGIC dropdown
# MAGIC
# MAGIC combobox
# MAGIC
# MAGIC multiselect
# MAGIC
# MAGIC remove
# MAGIC
# MAGIC removeAll
# MAGIC
# MAGIC 5. dbutils.library – Library/package management
# MAGIC
# MAGIC Install or stop libraries during execution.
# MAGIC
# MAGIC Functions:
# MAGIC
# MAGIC install()
# MAGIC
# MAGIC installPyPI()
# MAGIC
# MAGIC restartPython()
# MAGIC
# MAGIC 6. dbutils.jobs – Workflow task utilities
# MAGIC
# MAGIC Used in Jobs / LakeFlow workflows to pass values between tasks.
# MAGIC
# MAGIC Functions:
# MAGIC
# MAGIC taskValues.set
# MAGIC
# MAGIC taskValues.get
# MAGIC
# MAGIC taskValues.get(key, taskKey)
# MAGIC
# MAGIC 7. dbutils.credentials – Identity utilities
# MAGIC
# MAGIC Used to get token information when authenticating.
# MAGIC
# MAGIC Example functions:
# MAGIC
# MAGIC showRoles()
# MAGIC
# MAGIC showCurrentRole()
# MAGIC
# MAGIC (Some functions vary per cloud.)
# MAGIC
# MAGIC 8. dbutils.preview – Experimental utilities
# MAGIC
# MAGIC Used for experimental features Databricks is testing.
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC Experimental data profiler UI
# MAGIC
# MAGIC 9. dbutils.data (rarely used now, deprecated in some regions)
# MAGIC
# MAGIC Used earlier for data inspection.
# MAGIC
# MAGIC 🔥 Summary for Interview
# MAGIC
# MAGIC If someone asks "What utilities does dbutils provide?", respond:
# MAGIC
# MAGIC Databricks utilities help interact with platform services. The main utilities are:
# MAGIC
# MAGIC dbutils.fs for file system access
# MAGIC
# MAGIC dbutils.secrets for secure credentials
# MAGIC
# MAGIC dbutils.widgets for passing notebook parameters
# MAGIC
# MAGIC dbutils.notebook for orchestrating notebooks
# MAGIC
# MAGIC dbutils.jobs for sharing values between workflow tasks
# MAGIC
# MAGIC dbutils.library for managing libraries
# MAGIC
# MAGIC dbutils.credentials for authentication context
# MAGIC
# MAGIC ⭐ Quick one-line purpose of each
# MAGIC Utility	Purpose
# MAGIC fs	File system operations
# MAGIC notebook	Notebook orchestration
# MAGIC secrets	Secure credential handling
# MAGIC widgets	Notebook input parameters
# MAGIC jobs	Share values across job tasks
# MAGIC library	Manage installed libraries
# MAGIC credentials	Authentication + token info
# MAGIC preview	Experimental features

# COMMAND ----------


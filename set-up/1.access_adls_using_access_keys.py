# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Setup the spark configurationsfor access keys
# MAGIC 2. List the files from the demo container
# MAGIC 3. Read data from the circuit.csv file

# COMMAND ----------

formula1_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula3dluche.dfs.core.windows.net",
    formula1_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula3dluche.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula3dluche.dfs.core.windows.net/circuits.csv"))

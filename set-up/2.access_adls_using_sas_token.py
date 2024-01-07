# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS token
# MAGIC 1. Setup the spark configurations for SAS token
# MAGIC 2. List the files from the demo container
# MAGIC 3. Read data from the circuit.csv file

# COMMAND ----------

formula1_sas_token = dbutils.secrets.get(scope = 'formula1_sas_token', key = 'formula1-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dluche.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dluche.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dluche.dfs.core.windows.net", formula1_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dluche.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dluche.dfs.core.windows.net/circuits.csv"))

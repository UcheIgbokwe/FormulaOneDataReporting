# Databricks notebook source
# MAGIC %md
# MAGIC #### Access external storage account using Unity Catalog, External location and Storage Credentials
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC we do not need to specify the external storage account or credentials because Unity catalog handles that.

# COMMAND ----------

dbutils.fs.ls('abfss://demo@databrickscourseucdlext.dfs.core.windows.net/')

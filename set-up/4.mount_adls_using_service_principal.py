# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake using service principal
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set the Spark config with App/Client id, Directory/Tenant ID & Secret
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilties related to mount (list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'client_id_scope', key = 'client-id')
tenant_id = dbutils.secrets.get(scope = 'tenant_id_scope', key = 'tenant-id')
client_secret = dbutils.secrets.get(scope = 'client_secret_scope', key = 'client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}



# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dluche.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# Unmount the storage account container
dbutils.fs.unmount("/mnt/formula1dl/demo")

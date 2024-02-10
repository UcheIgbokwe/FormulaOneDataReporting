# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using service principal
# MAGIC 1. Register Azure AD Application/Service principal
# MAGIC 2. Generate a secret/password for the application
# MAGIC 3. Set the Spark config with App/Client id, Directory/Tenant ID & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake.

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'client_id_scope', key = 'client-id')
tenant_id = dbutils.secrets.get(scope = 'tenant_id_scope', key = 'tenant-id')
client_secret = dbutils.secrets.get(scope = 'client_secret_id_scope', key = 'client-secret-id')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula3dluche.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula3dluche.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula3dluche.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula3dluche.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula3dluche.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula3dluche.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula3dluche.dfs.core.windows.net/circuits.csv"))

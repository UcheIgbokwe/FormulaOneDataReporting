# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake Containers for the Project

# COMMAND ----------

# MAGIC %md
# MAGIC Important Note: When you mount an Azure Data Lake, it becomes associated with the configurations set at that specific time. Consequently, authentication is not required for subsequent ingestion and other actions. However, should there be a need to modify configurations such as the client secret or tenant ID, it is essential to unmount the Azure Data Lake and remount it accordingly.

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    client_id = dbutils.secrets.get(scope = 'formula4-scope', key = 'client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula4-scope', key = 'tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula4-scope', key = 'client-secret')

    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
               "fs.azure.account.oauth2.client.id": client_id,
               "fs.azure.account.oauth2.client.secret": client_secret,
               "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    # Check if container has already been mounted
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    # Mount the storage account container
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point=f"/mnt/{storage_account_name}/{container_name}",
        extra_configs=configs)

    # Display mounted containers
    display(dbutils.fs.mounts())


# COMMAND ----------

def un_mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    client_id = dbutils.secrets.get(scope = 'formula4-scope', key = 'client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula4-scope', key = 'tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula4-scope', key = 'client-secret')

    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
               "fs.azure.account.oauth2.client.id": client_id,
               "fs.azure.account.oauth2.client.secret": client_secret,
               "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    # UnMount the storage account container
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    # Display mounted containers
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('formula3dluche', 'raw')
#un_mount_adls('formula3dluche', 'raw')

# COMMAND ----------

mount_adls('formula3dluche', 'processed')
#un_mount_adls('formula3dluche', 'processed')

# COMMAND ----------

mount_adls('formula3dluche', 'presentation')
#un_mount_adls('formula3dluche', 'presentation')

# COMMAND ----------

mount_adls('formula3dluche', 'demo')
#un_mount_adls('formula3dluche', 'demo')

# COMMAND ----------

#un_mount_adls('formula3dl', 'demo')

# COMMAND ----------



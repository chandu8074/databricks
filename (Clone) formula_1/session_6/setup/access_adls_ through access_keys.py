# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

secret=dbutils.secrets.get(scope='formula1_scope',key='formula')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formulaadls1.dfs.core.windows.net",secret)

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formulaadls1.dfs.core.windows.net/formula1/"))

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    secret=dbutils.secrets.get(scope='formula1_scope',key='formula')
    
    # Set spark configurations
    configs = {"fs.azure.account.key.formulaadls1.blob.core.windows.net":secret}
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storage account container
    dbutils.fs.mount(
        source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('formulaadls1', 'presentation')

# COMMAND ----------



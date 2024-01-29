# Databricks notebook source
# MAGIC %md
# MAGIC #### Delta Lake Demo
# MAGIC
# MAGIC 1.Write data to delta lake (managed table)
# MAGIC
# MAGIC 2.Write data to delta lake (external table)
# MAGIC
# MAGIC 3.Read data from data lake (Table)
# MAGIC
# MAGIC 4.Read data from data lake (File)

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

query = f"CREATE DATABASE IF NOT EXISTS f1_demo LOCATION '{demo_folder_path}'"
spark.sql(query)

# COMMAND ----------

results_df = spark.read \
    .option("inferSchema", True) \
    .json(f"{raw_folder_path}/2021-03-28/results.json")

# COMMAND ----------

# Write data to DELTA TABLE directly to managed table
results_df.write.format("delta").mode("overwrite").saveAsTable(f"f1_demo.results_managed")

# COMMAND ----------

# Write data to DELTA TABLE directly external table
results_df.write.format("delta").mode("overwrite").save(f"{demo_folder_path}/results_external")

# COMMAND ----------

# Write partitioned data to DELTA TABLE directly external table
results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable(f"f1_demo.results_partitioned")

# COMMAND ----------

# Create an external table and push the data from the folder into it.
query = f"CREATE TABLE IF NOT EXISTS f1_demo.results_external USING DELTA LOCATION '{demo_folder_path}/results_external'"
spark.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read data from Delta lake table.
# MAGIC select * from f1_demo.results_partitioned

# COMMAND ----------

# Read data from delta lake via spark
results_external_df = spark.read.format("delta").load(f'{demo_folder_path}/results_external')

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

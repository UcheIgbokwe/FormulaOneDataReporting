# Databricks notebook source
# MAGIC %md
# MAGIC #### Delta Lake Demo
# MAGIC
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from data lake (Table)
# MAGIC 4. Read data from data lake (File)
# MAGIC 5. Update Delta table
# MAGIC 6. Delete from Delta table
# MAGIC 7. Upsert into Delta table

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

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, f"{demo_folder_path}/results_managed")

deltaTable.update("position <= 10", {"points" : "21 - position"})

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, f"{demo_folder_path}/results_managed")

deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %md
# MAGIC The upsert can be used when you need to insert new records, update old records or update and insert new records.
# MAGIC The 3 days show the different implementations.

# COMMAND ----------

drivers_day1_df = spark.read \
    .option("inferSchema", True) \
    .json(f"{raw_folder_path}/2021-03-28/drivers.json") \
    .filter("driverId <= 10") \
    .select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

#update records
from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
    .option("inferSchema", True) \
    .json(f"{raw_folder_path}/2021-03-28/drivers.json") \
    .filter("driverId BETWEEN 6 AND 15") \
    .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

#update and insert records
from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
    .option("inferSchema", True) \
    .json(f"{raw_folder_path}/2021-03-28/drivers.json") \
    .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
    .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC Day 1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC Day 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC Day 3

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import current_timestamp

deltaTable = DeltaTable.forPath(spark, f"{demo_folder_path}/drivers_merge")

deltaTable.alias('tgt') \
  .merge(
    drivers_day3_df.alias('upd'),
    'tgt.driverId = upd.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "tgt.dob": "upd.dob",
      "tgt.forename": "upd.forename",
      "tgt.surname": "upd.surname",
      "tgt.updatedDate": "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "tgt.driverId": "upd.driverId",
      "tgt.dob": "upd.dob",
      "tgt.forename": "upd.forename",
      "tgt.surname": "upd.surname",
      "tgt.createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet in Table to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet in File to Delta

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save(f"{demo_folder_path}/drivers_convert_to_delta_file")

# COMMAND ----------

file_path = f"{demo_folder_path}/drivers_convert_to_delta_file"
spark.sql(f"CONVERT TO DELTA parquet.`{file_path}`")

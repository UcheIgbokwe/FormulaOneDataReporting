# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest pitstops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1 - Read the multi-line JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", "true") \
.json("/mnt/formula1dluche/raw/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 2 - Rename the columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("diverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 3 - Write data to DataLake in Parquet

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite") \
    .parquet("/mnt/formula1dluche/processed/pitstops")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dluche/processed/pitstops
# MAGIC

# COMMAND ----------



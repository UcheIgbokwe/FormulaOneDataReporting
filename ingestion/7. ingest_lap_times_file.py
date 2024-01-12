# Databricks notebook source
# DBTITLE 1, Ingest laptimes.csv file
# MAGIC %md
# MAGIC #### Ingest laptimes.csv file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1 - Read the multiple CSV files using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 2 - Rename the columns and add ingestion date

# COMMAND ----------

lap_times_ingestion_date_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

lap_times_final_df = lap_times_ingestion_date_df.withColumnRenamed("diverId", "driver_id") \
.withColumnRenamed("raceId", "race_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 3 - Write data to DataLake in Parquet

# COMMAND ----------

lap_times_final_df.write.mode("overwrite") \
    .parquet(f"{processed_folder_path}/lap_times")

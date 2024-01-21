# Databricks notebook source
# DBTITLE 1,Ingest pitstops.json file
# MAGIC %md
# MAGIC #### Ingest pitstops.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/resource_file"

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
.json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 2 - Rename the columns and add ingestion date

# COMMAND ----------

pit_stops_add_ingestion_date_df = add_ingestion_date(pit_stops_df)

# COMMAND ----------

pit_stops_final_df = pit_stops_add_ingestion_date_df.withColumnRenamed("diverId", "driver_id") \
.withColumnRenamed("raceId", "race_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 3 - Write data to DataLake in Parquet

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite") \
    .format("parquet").saveAsTable("f1_processed.pitstops")

# COMMAND ----------

finalize_notebook()

# COMMAND ----------

dbutils.notebook.exit('Success')

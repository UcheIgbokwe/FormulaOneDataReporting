# Databricks notebook source
# DBTITLE 1, Ingest laptimes.csv file
# MAGIC %md
# MAGIC #### Ingest laptimes.csv file

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
.csv("/mnt/formula1dluche/raw/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 2 - Rename the columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("diverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 3 - Write data to DataLake in Parquet

# COMMAND ----------

lap_times_final_df.write.mode("overwrite") \
    .parquet("/mnt/formula1dluche/processed/lap_times")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dluche/processed/lap_times
# MAGIC

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1dluche/processed/lap_times'))

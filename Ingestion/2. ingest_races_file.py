# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ###### List all mounted files

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/formula1dluche/raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv("/mnt/formula1dluche/raw/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 2 - Add new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit, col, to_timestamp

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 3 - Select only required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("ingestion_date"), col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 4 - Rename the columns

# COMMAND ----------

races_final_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 5 - Write data to DataLake in Parquet

# COMMAND ----------

races_final_df.write.mode("overwrite") \
    .parquet("/mnt/formula1dluche/processed/races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dluche/processed/races

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dluche/processed/races"))

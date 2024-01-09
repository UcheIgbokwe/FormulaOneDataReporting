# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv("/mnt/formula1dluche/raw/circuits.csv")

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 2 - Select only required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 3 - Rename the columns

# COMMAND ----------

circuits_rename_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 4 - Add new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_rename_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 5 - Write data to DataLake in Parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1dluche/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dluche/processed/circuits

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dluche/processed/circuits"))

# COMMAND ----------



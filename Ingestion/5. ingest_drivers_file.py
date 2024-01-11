# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json("/mnt/formula1dluche/raw/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 2 - Rename the columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit, col

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed("diverId", "driver_id") \
.withColumnRenamed("driverRef", "driver_ref") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")) )

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 3 - Drop unwanted column

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop("url") \
.drop("forename") \
.drop("surname")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 4 - Write data to DataLake in Parquet

# COMMAND ----------

drivers_final_df.write.mode("overwrite") \
    .parquet("/mnt/formula1dluche/processed/drivers")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dluche/processed/drivers
# MAGIC

# COMMAND ----------



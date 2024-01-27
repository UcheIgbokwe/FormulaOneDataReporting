# Databricks notebook source
# DBTITLE 1,Ingest results.json file
# MAGIC %md
# MAGIC #### Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/resource_file"

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1 - Read the single-line JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType([
        StructField('resultId', IntegerType(), False),
        StructField('raceId', IntegerType(), False),
        StructField('driverId', IntegerType(), False),
        StructField('constructorId', IntegerType(), False),
        StructField('number', IntegerType(), True),
        StructField('grid', IntegerType(), False),
        StructField('position', IntegerType(), True),
        StructField('positionText', StringType(), False),
        StructField('positionOrder', IntegerType(), False),
        StructField('points', FloatType(), False),
        StructField('laps', IntegerType(), False),
        StructField('time', StringType(), True),
        StructField('milliseconds', IntegerType(), True),
        StructField('fastestLap', IntegerType(), True),
        StructField('rank', IntegerType(), True),
        StructField('fastestLapTime', IntegerType(), True),
        StructField('fastestLapSpeed', StringType(), True),
        StructField('statusId', IntegerType(), False)
])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 2 - Rename the columns and add ingestion date

# COMMAND ----------

results_add_ingestion_date_df = add_ingestion_date(results_df)

# COMMAND ----------

results_dropped_df = results_add_ingestion_date_df.drop('statusId')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_final_df = results_dropped_df.withColumnRenamed("resultId", "result_id") \
        .withColumnRenamed("raceId", "race_id") \
        .withColumnRenamed("driverId", "driver_id") \
        .withColumnRenamed("constructorId", "constructor_id") \
        .withColumnRenamed("positionText", "position_text") \
        .withColumnRenamed("positionOrder", "position_order") \
        .withColumnRenamed("fastestLap", "fastest_lap") \
        .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
        .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
        .withColumn("ingestion_date", current_timestamp()) \
        .withColumn("data_source", lit(v_data_source)) \
        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 3 - Write data to DataLake in Parquet

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Method 1

# COMMAND ----------

# # Check if the table exists first
# if spark._jsparkSession.catalog().tableExists("f1_processed.results"):
#     # Create a list of race_ids
#     race_ids = [row.race_id for row in results_final_df.select("race_id").distinct().collect()]

#     # Construct the SQL query to drop partitions
#     drop_partitions_query = "ALTER TABLE f1_processed.results DROP IF EXISTS "
#     drop_partitions_query += ", ".join([f"PARTITION (race_id = {race_id})" for race_id in race_ids])

#     # Execute the query
#     spark.sql(drop_partitions_query)


# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id") \
#     .format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Method 2

# COMMAND ----------

results_final_df = move_columns_to_end(results_final_df, ["race_id"])

# COMMAND ----------

write_to_table(spark, results_final_df, "f1_processed.results", "race_id", dynamic_partition=True)

# COMMAND ----------

finalize_notebook()

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM f1_processed.results;
# MAGIC SELECT race_id, count(1) FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

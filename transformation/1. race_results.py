# Databricks notebook source
# MAGIC %md
# MAGIC #### Race Results

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

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name", "team")

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
    .filter(f"file_date == '{v_file_date}'") \
    .withColumnRenamed("time", "race_time") \
    .withColumnRenamed("race_id", "result_race_id") \
    .withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

# Join circuit to races
race_circuit_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id , "inner") \
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location,)

# COMMAND ----------

# Join results to other dataframes
race_results_df = results_df \
    .join(race_circuit_df, results_df.result_race_id == race_circuit_df.race_id, "inner") \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select('race_id','race_year', 'race_name', 'circuit_location', 
    'driver_name', 'driver_number', 'driver_nationality','team', 'grid', 'fastest_lap','race_time', 'points','position', 'result_file_date') \
    .withColumn('created_date', current_timestamp()) \
    .withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

#display(final_df.filter('race_year == 2020 and race_name == "Abu Dhabi Grand Prix"').orderBy(final_df.points.desc()))
#display(final_df)

# COMMAND ----------

process_and_write_to_table(spark, final_df, "f1_presentation.race_results", "race_id", ["race_id"], dynamic_partition=True)

# COMMAND ----------

finalize_notebook()

# COMMAND ----------

dbutils.notebook.exit('Success')

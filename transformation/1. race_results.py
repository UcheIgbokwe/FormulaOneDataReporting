# Databricks notebook source
# MAGIC %md
# MAGIC #### Race Results

# COMMAND ----------

# MAGIC %run "../includes/configuration"

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
    .withColumnRenamed("time", "race_time")

# COMMAND ----------

# Join circuit to races
race_circuit_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id , "inner") \
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location,)

# COMMAND ----------

# Join results to other dataframes
race_results_df = results_df \
    .join(race_circuit_df, results_df.race_id == race_circuit_df.race_id, "inner") \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select('race_year', 'race_name', 'circuit_location', 
    'driver_name', 'driver_number', 'driver_nationality','team', 'grid', 'fastest_lap','race_time', 'points','position') \
    .withColumn('created_date', current_timestamp())

# COMMAND ----------

#display(final_df.filter('race_year == 2020 and race_name == "Abu Dhabi Grand Prix"').orderBy(final_df.points.desc()))
#display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite") \
    .parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

finalize_notebook()

# COMMAND ----------

dbutils.notebook.exit('Success')

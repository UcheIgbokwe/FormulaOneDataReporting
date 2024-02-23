# Databricks notebook source
# MAGIC %md
# MAGIC #### Driver standings

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

from delta.tables import DeltaTable

# COMMAND ----------

# Find race year from which data is to be reprocessed
race_results_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date = '{v_file_date}'")


# COMMAND ----------

race_year_list = df_column_to_list(race_results_list, 'race_year')    

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

# Add number of wins
from pyspark.sql.functions import sum, count, when, col

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality") \
.agg(sum("points").alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

# Add rankings to the driver standings
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driverRankSpec))

# COMMAND ----------

#process_and_write_to_table(spark, final_df, "f1_presentation.driver_standings", "race_year", ["race_year"], dynamic_partition=True)
merge_condition = "tgt.race_year = src.race_year AND tgt.driver_name = src.driver_name "
merge_delta_data(final_df, "f1_presentation.driver_standings", presentation_folder_path, "driver_standings", "race_year", merge_condition)

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings

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
race_results_list = spark.read.parquet(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_list, 'race_year')

# COMMAND ----------

# Read the data from parquet file
from pyspark.sql.functions import col

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

# Convert the DataFrame to Delta format
race_results_df.write.format("delta").mode("overwrite").save(f"{delta_folder_path}/race_results_delta")

# COMMAND ----------

# Create a DeltaTable object
delta_table = DeltaTable.forPath(spark, f"{delta_folder_path}/race_results_delta")

# COMMAND ----------

# Convert data to dataframe
race_results_delta_df = delta_table.toDF()

# COMMAND ----------

# Add number of wins
from pyspark.sql.functions import sum, count, when, col

constructor_standings_df = race_results_delta_df \
.groupBy("race_year","team") \
.agg(sum("points").alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

# Add rankings to the driver standings
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

constructorRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructorRankSpec))

# COMMAND ----------

process_and_write_to_table(spark, final_df, "f1_presentation.constructor_standings", "race_year", ["race_year"], dynamic_partition=True)

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructor_standings

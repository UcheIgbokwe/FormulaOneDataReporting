# Databricks notebook source
# MAGIC %md
# MAGIC #### Driver standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/resource_file"

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# Read the data from parquet file
race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

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

driver_standings_df = race_results_delta_df \
.groupBy("race_year", "driver_name", "driver_nationality", "team") \
.agg(sum("points").alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

# Add rankings to the driver standings
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driverRankSpec))

# COMMAND ----------

final_df.write.mode("overwrite") \
    .format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

dbutils.notebook.exit('Success')

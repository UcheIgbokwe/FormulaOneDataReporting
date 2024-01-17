# Databricks notebook source
# MAGIC %md
# MAGIC #### Global Temporary Views
# MAGIC
# MAGIC 1.Create global temporary views on dataframes 
# MAGIC
# MAGIC 2.Access the view from SQL cell 
# MAGIC
# MAGIC 3.Access the view from Python cell 
# MAGIC
# MAGIC 4.Access the view from another notebook

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from delta.tables import DeltaTable
# Convert the DataFrame to Delta format
race_results_df.write.format("delta").mode("overwrite").save(f"{delta_folder_path}/race_results_delta")
# Create a DeltaTable object
delta_table = DeltaTable.forPath(spark, f"{delta_folder_path}/race_results_delta")
# Convert data to dataframe
race_results_delta_df = delta_table.toDF()

# COMMAND ----------

race_results_delta_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

race_results_2019_df = spark.sql("SELECT * FROM gv_race_results WHERE race_year = 2019")

# COMMAND ----------

display(race_results_2019_df)

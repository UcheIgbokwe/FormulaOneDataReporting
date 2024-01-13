# Databricks notebook source
# MAGIC %md
# MAGIC #### Spark Filter & Where clause Transformation

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

#single parameter
#races_filtered_df= races_df.filter("race_year = 2019")
#using filter clause
#races_filtered_df= races_df.where("race_year = 2019 and round <= 5")
#using where clause
races_filtered_df= races_df.where("race_year = 2019 and round <= 5")

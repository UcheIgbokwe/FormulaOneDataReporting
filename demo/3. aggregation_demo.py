# Databricks notebook source
# MAGIC %md
# MAGIC #### Spark Aggregation using DeltaTable for faster reading.

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# Read the data from parquet file
demo_race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# Convert the DataFrame to Delta format
demo_race_results_df.write.format("delta").mode("overwrite").save(f"{delta_folder_path}/demo_race_results_delta")

# COMMAND ----------

# Create a DeltaTable object
delta_table = DeltaTable.forPath(spark, f"{delta_folder_path}/demo_race_results_delta")

# COMMAND ----------

# Filter the data for race year 2020
filtered_2020_df = delta_table.toDF().filter("race_year = 2020")

# COMMAND ----------

from pyspark.sql.functions import countDistinct, sum

# COMMAND ----------

filtered_2020_df.select(countDistinct("race_name")).show()

# COMMAND ----------

filtered_2020_df.select(sum("points")).show()

# COMMAND ----------

filtered_2020_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points")) \
.withColumnRenamed("sum(points)", "total_points") \
.show()

# COMMAND ----------

filtered_2020_df \
.filter("driver_name = 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name")) \
.withColumnRenamed("sum(points)", "total_points") \
.withColumnRenamed("count(DISTINCT race_name)", "number_of_races") \
.show()

# COMMAND ----------

filtered_2020_df \
.groupBy("driver_name") \
.agg(sum("points"), countDistinct("race_name")) \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Window functions

# COMMAND ----------

# Filter the data for race year 2020 and 2019
filtered_2019_2020_df = delta_table.toDF().filter("race_year in (2019, 2020)")

# COMMAND ----------

demo_grouped_df = filtered_2019_2020_df \
.groupBy("race_year","driver_name") \
.agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grouped_df.withColumn("rank", rank().over(driverRankSpec)).show(100)

# COMMAND ----------

# Clear the resources
filtered_2020_df.unpersist()
filtered_2019_2020_df.unpersist()

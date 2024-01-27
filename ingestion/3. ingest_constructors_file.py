# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/resource_file"

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructors_schema= "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 2 - Drop unwanted column

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 3 - Rename the columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructors_renamed_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 4 - Write data to DataLake in Parquet

# COMMAND ----------

constructors_final_df.write.mode("overwrite") \
    .format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

finalize_notebook()

# COMMAND ----------

dbutils.notebook.exit('Success')

-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create Presentation DB and Transform all files

-- COMMAND ----------

-- MAGIC %run "../utils/drop_and_create_all_tables"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drop_create_database('presentation')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.run("1. race_results", 0)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.run("2. driver_standings", 0)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.run("3. constructor_standings", 0)

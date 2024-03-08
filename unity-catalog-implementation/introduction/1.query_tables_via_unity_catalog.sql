-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Query data via unity catalog using 3 level namespace
-- MAGIC

-- COMMAND ----------

SELECT * FROM demo_catalog.demo_schema_test.circuits

-- COMMAND ----------

USE CATALOG demo_catalog;
USE SCHEMA demo_schema_test;
SELECT * FROM circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Using Python to display data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table('demo_catalog.demo_schema_test.circuits')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

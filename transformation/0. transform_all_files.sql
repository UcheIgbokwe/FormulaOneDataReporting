-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create Presentation DB and Transform all files

-- COMMAND ----------

DROP SCHEMA IF EXISTS f1_presentation CASCADE;
DROP DATABASE IF EXISTS f1_presentation;
CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1dluche/presentation";

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.run("1. race_results", 0)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.run("2. driver_standings", 0)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.run("3. constructor_standings", 0)

-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create Catalogs and Schemas required for this project
-- MAGIC 1. Catalog - formula1dev (without managed location)
-- MAGIC 2. Schemas - bronze, silver, gold (with managed location)

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS `formula1_dev`;

-- COMMAND ----------

USE CATALOG formula1_dev;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS `bronze`
MANAGED LOCATION 'abfss://bronze@databrickscourseucdlext.dfs.core.windows.net/'

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS `silver`
MANAGED LOCATION 'abfss://silver@databrickscourseucdlext.dfs.core.windows.net/'

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS `gold`
MANAGED LOCATION 'abfss://gold@databrickscourseucdlext.dfs.core.windows.net/'

-- COMMAND ----------

SHOW SCHEMAS

-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create the external locations required for this project
-- MAGIC 1. Bronze
-- MAGIC 2. Silver
-- MAGIC 3. Gold

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `databrickscourseucdlext_bronze`
URL 'abfss://bronze@databrickscourseucdlext.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databrickscourse-ext-storage-credential`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `databrickscourseucdlext_silver`
URL 'abfss://silver@databrickscourseucdlext.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databrickscourse-ext-storage-credential`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `databrickscourseucdlext_gold`
URL 'abfss://gold@databrickscourseucdlext.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databrickscourse-ext-storage-credential`);

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls "abfss://bronze@databrickscourseucdlext.dfs.core.windows.net/"

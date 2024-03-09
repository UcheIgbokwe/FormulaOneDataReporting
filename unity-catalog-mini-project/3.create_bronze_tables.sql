-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create Bronze tables
-- MAGIC 1. drivers.json
-- MAGIC 2. results.json
-- MAGIC 3. Bronze folder path - abfss://bronze@databrickscourseucdlext.dfs.core.windows.net/

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_dev.bronze.drivers;

CREATE TABLE IF NOT EXISTS formula1_dev.bronze.drivers
(
  driverId INT, 
  driverRef STRING, 
  number INT, 
  code STRING, 
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING, 
  url STRING
)
USING json
OPTIONS (path "abfss://bronze@databrickscourseucdlext.dfs.core.windows.net/drivers.json");

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_dev.bronze.results;

CREATE TABLE IF NOT EXISTS formula1_dev.bronze.results
(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT, 
  number INT, grid INT, 
  position INT, 
  positionText STRING,
  positionOrder INT, 
  points INT, 
  laps INT, 
  time STRING,
  milliseconds INT, 
  fastestLap INT, 
  rank INT, 
  fastestLapTime INT, 
  fastestLapSpeed STRING,
  statusId INT
)
USING json
OPTIONS (path "abfss://bronze@databrickscourseucdlext.dfs.core.windows.net/results.json");

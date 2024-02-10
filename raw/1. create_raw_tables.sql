-- Databricks notebook source
DROP DATABASE IF EXISTS f1_raw;
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create circuits table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.circuits( 
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS(path "/mnt/formula3dluche/raw/circuits.csv", header true)

-- COMMAND ----------

--SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create races table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races( 
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS(path "/mnt/formula3dluche/raw/races.csv", header true)

-- COMMAND ----------

--SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create constructors table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.constructors( 
  constructorId INT, 
  constructorRef STRING, 
  name STRING, 
  nationality STRING, 
  url STRING
)
USING json
OPTIONS(path "/mnt/formula3dluche/raw/constructors.json")

-- COMMAND ----------

--SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create drivers table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.drivers( 
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
OPTIONS(path "/mnt/formula3dluche/raw/drivers.json")

-- COMMAND ----------

--SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create results table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.results( 
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
OPTIONS(path "/mnt/formula3dluche/raw/results.json")

-- COMMAND ----------

--SELECT * FROM f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create pit stops table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.pitstops( 
  raceId INT,
  driverId INT,
  stop INT,
  laps INT, 
  time STRING,
  duration STRING, 
  milliseconds INT
)
USING json
OPTIONS(path "/mnt/formula3dluche/raw/pit_stops.json", multiLine true)


-- COMMAND ----------

--SELECT * FROM f1_raw.pitstops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create Lap Times Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.lap_times( 
  raceId INT,
  driverId INT,
  lap INT, 
  position INT, 
  time STRING,
  milliseconds INT
)
USING csv
OPTIONS(path "/mnt/formula3dluche/raw/lap_times")

-- COMMAND ----------

-- SELECT * FROM f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create Qualifying Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.qualifying( 
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT, 
  number INT,
  position INT,
  q1 STRING,
  q2 STRING, 
  q3 STRING
)
USING json
OPTIONS(path "/mnt/formula3dluche/raw/qualifying", multiLine true)

-- COMMAND ----------

--SELECT * FROM f1_raw.qualifying;
--desc extended f1_raw.qualifying; --THIS TELLS YOU IF IT IS EXTERNAL OR MANAGED TABLE

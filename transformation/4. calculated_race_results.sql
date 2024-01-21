-- Databricks notebook source
-- MAGIC %python
-- MAGIC from delta.tables import DeltaTable

-- COMMAND ----------

-- Use the target database
USE f1_processed;

CONVERT TO DELTA circuits;
CONVERT TO DELTA results;
CONVERT TO DELTA races;
CONVERT TO DELTA constructors;
CONVERT TO DELTA drivers;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("""
-- MAGIC                 SELECT races.race_year,
-- MAGIC                       constructors.name AS team_name,
-- MAGIC                       drivers.name AS driver_name,
-- MAGIC                       results.position,
-- MAGIC                       results.points,
-- MAGIC                       11 - results.position AS calculated_points
-- MAGIC                 FROM results
-- MAGIC                 JOIN drivers ON (results.driver_id = drivers.driver_id)
-- MAGIC                 JOIN constructors ON (results.constructor_id = constructors.constructor_id)
-- MAGIC                 JOIN races ON (results.race_id = races.race_id)    
-- MAGIC                 WHERE results.position <= 10;          
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("delta").mode("overwrite").saveAsTable("f1_presentation.calculated_race_results")

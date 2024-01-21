# Databricks notebook source
# MAGIC %md
# MAGIC #### Notebook workflow

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS f1_processed CASCADE;
# MAGIC DROP DATABASE IF EXISTS f1_processed;
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed
# MAGIC LOCATION "/mnt/formula1dluche/processed";

# COMMAND ----------

v_result = dbutils.notebook.run("1. ingest_circuits_file", 0)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2. ingest_races_file", 0)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3. ingest_constructors_file", 0)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4. ingest_drivers_file", 0)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5. ingest_pit_stops_file", 0)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6. ingest_lap_times_file", 0)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7. ingest_qualifying_file", 0)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8. ingest_results_file", 0)

# COMMAND ----------

v_result

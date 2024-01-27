# Databricks notebook source
# MAGIC %md
# MAGIC #### Drop and Create all the tables

# COMMAND ----------

def drop_create_database(parameter):
    db_name = 'f1_' + parameter

    # Drop the schema if it exists
    spark.sql(f'DROP SCHEMA IF EXISTS {db_name} CASCADE')

    # Drop the database if it exists
    spark.sql(f'DROP DATABASE IF EXISTS {db_name}')

    # Create the new database
    location = f"/mnt/formula1dluche/{parameter}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{location}'")

    return f'Database {db_name} created successfully at {location}'


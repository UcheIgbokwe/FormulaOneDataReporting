# Databricks notebook source
df = spark.sql("""
    SELECT 
        driver_name,
        COUNT(1) AS total_races,
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points
    FROM f1_presentation.calculated_race_results
    GROUP BY driver_name
    HAVING COUNT(1) >= 50
    ORDER BY avg_points DESC;          
""")
display(df)

# COMMAND ----------


df_by_raceyear = spark.sql("""
    SELECT 
        driver_name,
        COUNT(1) AS total_races,
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points
    FROM f1_presentation.calculated_race_results
    WHERE race_year BETWEEN 2010 AND 2011
    GROUP BY driver_name
    HAVING COUNT(1) >= 50
    ORDER BY avg_points DESC;          
""")
display(df_by_raceyear)

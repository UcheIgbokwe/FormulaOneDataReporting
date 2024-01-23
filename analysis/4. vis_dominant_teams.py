# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_dominant_teams AS
# MAGIC SELECT 
# MAGIC         team_name,
# MAGIC         COUNT(1) AS total_races,
# MAGIC         SUM(calculated_points) AS total_points,
# MAGIC         AVG(calculated_points) AS avg_points,
# MAGIC         RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS team_rank
# MAGIC     FROM f1_presentation.calculated_race_results
# MAGIC     GROUP BY team_name
# MAGIC     HAVING COUNT(1) >= 100
# MAGIC     ORDER BY avg_points DESC; 

# COMMAND ----------

df = spark.sql("""
    SELECT 
        race_year,
        team_name,
        COUNT(1) AS total_races,
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points
    FROM f1_presentation.calculated_race_results
    WHERE team_name in (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
    GROUP BY race_year, team_name
    ORDER BY race_year, avg_points DESC;          
""")
display(df)


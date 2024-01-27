# Databricks notebook source
html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers </h1>"""
displayHTML(html)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_dominant_drivers AS
# MAGIC SELECT 
# MAGIC         driver_name,
# MAGIC         COUNT(1) AS total_races,
# MAGIC         SUM(calculated_points) AS total_points,
# MAGIC         AVG(calculated_points) AS avg_points,
# MAGIC         RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS driver_rank
# MAGIC     FROM f1_presentation.calculated_race_results
# MAGIC     GROUP BY driver_name
# MAGIC     HAVING COUNT(1) >= 50
# MAGIC     ORDER BY avg_points DESC;

# COMMAND ----------

df = spark.sql("""
    SELECT 
        race_year,
        driver_name,
        COUNT(1) AS total_races,
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points
    FROM f1_presentation.calculated_race_results
    WHERE driver_name in (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
    GROUP BY race_year, driver_name
    ORDER BY race_year, avg_points DESC;          
""")
display(df)


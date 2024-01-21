# Databricks notebook source
df = spark.sql("""
                SELECT *
                FROM f1_presentation.calculated_race_results;          
""")
display(df)

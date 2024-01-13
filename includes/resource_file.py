# Databricks notebook source
from pyspark.dbutils import DBUtils

def finalize_notebook():
    # Create a DBUtils instance
    dbutils = DBUtils(spark)

    # Get the notebook path
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

    # Extract just the notebook name
    notebook_name = notebook_path.split('/')[-1]

    # Print the completion message with the notebook name
    print(f"Notebook {notebook_name} action completed.")

# COMMAND ----------



# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

# MAGIC %md
# MAGIC The function below is used to save parquet data into Delta tables using partition and merge

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from delta.tables import *

def merge_delta_data(df: DataFrame, table_name: str, folder_path: str, folder_name: str, partition_column: str, merge_condition: str):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

    if spark._jsparkSession.catalog().tableExists(table_name):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{folder_name}")
        deltaTable.alias('tgt') \
        .merge(
            df.alias('src'),
            merge_condition
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    else:
        df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(table_name)    

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame

def process_and_write_to_table(spark: SparkSession, df: DataFrame, table_name: str, partition_column: str, columns_to_move: list, dynamic_partition: bool = True):
    """
    Processes a DataFrame by moving specified columns to the end, 
    and then writes it to a table. If the table exists, it overwrites the data. 
    If not, it creates a new table with the specified partition.
    Can optionally use dynamic partition overwrite mode.

    Parameters:
    spark (SparkSession): The SparkSession object.
    df (DataFrame): The DataFrame to process and write.
    table_name (str): The name of the table.
    partition_column (str): The column to partition the table by.
    columns_to_move (list): List of column names to move to the end.
    dynamic_partition (bool): Whether to use dynamic partition overwrite mode.
    """
    # Move specified columns to the end of the DataFrame
    all_columns = df.columns
    remaining_columns = [col for col in all_columns if col not in columns_to_move]
    reordered_columns = remaining_columns + columns_to_move
    df = df.select(reordered_columns)

    # Set dynamic partition overwrite mode if needed
    if dynamic_partition:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # Write the DataFrame to the table
    if spark._jsparkSession.catalog().tableExists(table_name):
        df.write.mode("overwrite").insertInto(table_name)
    else:
        df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(table_name)

# COMMAND ----------

def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name) \
                            .distinct() \
                            .collect()

    # PYTHON WAY OF DOING AN APPEND
    column_value_list= [row[column_name] for row in df_row_list]
    return column_value_list                        

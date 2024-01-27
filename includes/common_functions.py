# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

from pyspark.sql import DataFrame
def move_columns_to_end(df: DataFrame, columns_to_move: list) -> DataFrame:
    """
    Move specified columns to the end of a DataFrame.

    Parameters:
    df (DataFrame): The DataFrame to reorder.
    columns_to_move (list): List of column names to move to the end.

    Returns:
    DataFrame: The reordered DataFrame.
    """
    # Get the list of all columns
    all_columns = df.columns
    
    # Filter out the columns to move, keeping the order of the remaining columns
    remaining_columns = [col for col in all_columns if col not in columns_to_move]
    
    # Combine the remaining columns with the columns to move
    reordered_columns = remaining_columns + columns_to_move
    
    # Select columns in the new order
    return df.select(reordered_columns)

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
def write_to_table(spark: SparkSession, df: DataFrame, table_name: str, partition_column: str, dynamic_partition: bool = True):
    """
    Writes a DataFrame to a table. If the table exists, it overwrites the data. 
    If not, it creates a new table with the specified partition.
    Can optionally use dynamic partition overwrite mode.

    Parameters:
    spark (SparkSession): The SparkSession object.
    df (DataFrame): The DataFrame to write.
    table_name (str): The name of the table.
    partition_column (str): The column to partition the table by.
    dynamic_partition (bool): Whether to use dynamic partition overwrite mode.
    """
    if dynamic_partition:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if spark._jsparkSession.catalog().tableExists(table_name):
        df.write.mode("overwrite").insertInto(table_name)
    else:
        df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(table_name)

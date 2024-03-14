# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def common_func(input_df):
    output_df=input_df.withColumn("ingestion_date",current_timestamp())
    return output_df

# COMMAND ----------



# Databricks notebook source
dbutils.fs.ls("dbfs:/mnt/formulaadls1/processed/circuits/")

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

circuit_df=spark.read.csv("dbfs:/mnt/formulaadls1/raw/formula1/circuits.csv",schema=circuits_schema)

# COMMAND ----------

display(circuit_df)

# COMMAND ----------

circuit_df=circuit_df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("lat","latitude").withColumnRenamed("lng","longitude")

# COMMAND ----------

circuit_df=circuit_df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

circuit_df.display()

# COMMAND ----------

circuit_df.write.mode("overwrite").parquet("dbfs:/mnt/formulaadls1/processed/circuits")

# COMMAND ----------

display(spark.read.parquet("dbfs:/mnt/formulaadls1/processed/circuits"))

# COMMAND ----------



# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/formulaadls1/raw/formula1/")

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv("dbfs:/mnt/formulaadls1/raw/formula1/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

races_df=races_df.withColumn("ingest_date",current_timestamp()).withColumn("races_timestamp",to_timestamp(concat(col("date"),lit(' '),col("time")),"yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

races_df.display()

# COMMAND ----------

races_df.write.mode("overwrite").partitionBy("year").parquet("dbfs:/mnt/formulaadls1/processed/races")

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/formulaadls1/processed/races", recurse=True)

# COMMAND ----------



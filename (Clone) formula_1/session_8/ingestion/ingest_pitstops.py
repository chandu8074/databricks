# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df=spark.read.format("json").schema(pit_stops_schema).option("multiline",True).json("dbfs:/mnt/formulaadls1/raw/formula1/pit_stops.json")

# COMMAND ----------

pit_stops_df.display()

# COMMAND ----------

pit_stops_df=pit_stops_df.withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

pit_stops_df.display()

# COMMAND ----------

pit_stops_df.write.mode("overwrite").parquet("dbfs:/mnt/formulaadls1/processed/pit_stops")

# COMMAND ----------



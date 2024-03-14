# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/formulaadls1/processed")

# COMMAND ----------

constructor_df=spark.read.option("header",True).schema(constructor_schema).json("dbfs:/mnt/formulaadls1/raw/formula1/constructors.json")

# COMMAND ----------

constructor_schema=StructType(fields=[StructField("constructorId",IntegerType(),True),StructField("constructorRef",StringType(),True),StructField("name",StringType(),True),StructField("nationality",StringType(),True),StructField("url",StringType(),True)
])

# COMMAND ----------

constructor_df=spark.read.option("header",True).schema(constructor_schema).json("dbfs:/mnt/formulaadls1/raw/formula1/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_df=constructor_df.withColumnRenamed("constructorId","constructor_id").withColumnRenamed("constructorRef","constructor_ref").withColumn("ingestion_date",current_timestamp()).drop("url")

# COMMAND ----------

constructor_df.display()

# COMMAND ----------

constructor_df.write.mode("overwrite").parquet("dbfs:/mnt/formulaadls1/processed/constructors")

# COMMAND ----------



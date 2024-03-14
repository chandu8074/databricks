# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

name_schema=StructType(fields=[
    StructField("forename",StringType(),True),
    StructField("surname",StringType(),True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

drivers_df=spark.read.schema(drivers_schema).json("dbfs:/mnt/formulaadls1/raw/formula1/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_df=drivers_df.withColumnRenamed("driverId","driver_id").withColumnRenamed("driverRef","driver_ref").withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname"))).drop("url").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

drivers_df.display()

# COMMAND ----------

drivers_df.write.mode("overwrite").parquet("dbfs:/mnt/formulaadls1/processed/drivers")

# COMMAND ----------



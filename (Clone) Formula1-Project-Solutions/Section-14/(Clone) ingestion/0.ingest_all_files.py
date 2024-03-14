# Databricks notebook source
v_res=dbutils.notebook.run("1.ingest_circuits_file",0,{"datasource":"Ergast API"})

# COMMAND ----------

v_res

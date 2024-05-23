# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS landing

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS landing.power

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS landing.power.turbine_raw_landing

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import json

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

# COMMAND ----------

catalog = 'eni_databricks_corsobase_dataanalysts'
schema = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson())["attributes"]["user"].split('@')[0].replace('.', '_')

# COMMAND ----------

spark.sql(f'CREATE CATALOG IF NOT EXISTS {catalog}')

# COMMAND ----------

spark.sql(f'CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}')

# COMMAND ----------

dbutils.fs.mkdirs('/Volumes/landing/power/turbine_raw_landing/engines1')
dbutils.fs.mkdirs('/Volumes/landing/power/turbine_raw_landing/historical_turbine_status1')
dbutils.fs.mkdirs('/Volumes/landing/power/turbine_raw_landing/incoming_data1')
dbutils.fs.mkdirs('/Volumes/landing/power/turbine_raw_landing/parts1')
dbutils.fs.mkdirs('/Volumes/landing/power/turbine_raw_landing/turbines1')

# COMMAND ----------

import shutil

shutil.copyfile('_resources/engine_1.json', '/Volumes/landing/power/turbine_raw_landing/engines1/engine_1.json')
shutil.copyfile('_resources/parts.json', '/Volumes/landing/power/turbine_raw_landing/parts1/parts.json')
shutil.copyfile('_resources/part-00000-tid-7199961357610813993-5fe2ea1d-2309-4f12-95e0-ae58664ea53a-857-1-c000.csv', '/Volumes/landing/power/turbine_raw_landing/incoming_data1/part-00000-tid-7199961357610813993-5fe2ea1d-2309-4f12-95e0-ae58664ea53a-857-1-c000.csv')
shutil.copyfile('_resources/turbine_1.csv', '/Volumes/landing/power/turbine_raw_landing/turbines1/turbine_1.json')

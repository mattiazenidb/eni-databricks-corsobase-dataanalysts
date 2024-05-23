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

# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS eni_databricks_corsobase_dataanalysts

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS eni_databricks_corsobase_dataanalysts.course_schema

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

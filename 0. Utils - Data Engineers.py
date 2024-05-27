# Databricks notebook source
catalog = 'eni_databricks_corsobase_dataanalysts'

# COMMAND ----------

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
# MAGIC GRANT USE CATALOG ON CATALOG landing TO `account users`;
# MAGIC GRANT USE SCHEMA ON SCHEMA landing.power TO `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS landing.power.turbine_raw_landing

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC GRANT READ VOLUME ON VOLUME landing.power.turbine_raw_landing TO `account users`;

# COMMAND ----------

spark.sql(f'CREATE CATALOG IF NOT EXISTS {catalog}')
spark.sql(f'GRANT USE CATALOG ON CATALOG {catalog} TO `account users`')

# COMMAND ----------

dbutils.fs.mkdirs('/Volumes/landing/power/turbine_raw_landing/engines')
dbutils.fs.mkdirs('/Volumes/landing/power/turbine_raw_landing/historical_turbine_status')
dbutils.fs.mkdirs('/Volumes/landing/power/turbine_raw_landing/incoming_data')
dbutils.fs.mkdirs('/Volumes/landing/power/turbine_raw_landing/parts')
dbutils.fs.mkdirs('/Volumes/landing/power/turbine_raw_landing/turbines')

# COMMAND ----------

import shutil

shutil.copyfile('_resources/engine_1.json', '/Volumes/landing/power/turbine_raw_landing/engines/engine_1.json')
shutil.copyfile('_resources/parts.json', '/Volumes/landing/power/turbine_raw_landing/parts/parts.json')
shutil.copyfile('_resources/part-00000-tid-5635562292802017934-c3e75aed-50bc-4000-bf88-2d2ef34bd1d9-130-1-c000.csv', '/Volumes/landing/power/turbine_raw_landing/incoming_data/part-00000-tid-5635562292802017934-c3e75aed-50bc-4000-bf88-2d2ef34bd1d9-130-1-c000.csv')
shutil.copyfile('_resources/turbine_1.csv', '/Volumes/landing/power/turbine_raw_landing/turbines/turbine_1.json')

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

# MAGIC %md
# MAGIC
# MAGIC Catalogo sara' fornito dai team di data engineer

# COMMAND ----------

schema = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson())["attributes"]["user"].split('@')[0].replace('.', '_')

# COMMAND ----------

spark.sql(f'CREATE CATALOG IF NOT EXISTS {catalog}')

# COMMAND ----------

spark.sql(f'CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}')

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

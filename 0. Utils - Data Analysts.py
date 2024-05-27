# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Note
# MAGIC Il catalogo ```catalog``` sara' fornito dai team di data engineer

# COMMAND ----------

catalog = 'eni_databricks_corsobase_dataanalysts'

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import json

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

# COMMAND ----------

schema = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson())["attributes"]["user"].split('@')[0].replace('.', '_')

# COMMAND ----------

spark.sql(f'CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}')

# Databricks notebook source
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

# MAGIC %md
# MAGIC
# MAGIC ## In Databricks you can run any custom python code. This includes code to read from external sources such as an API

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## I can read from a REST API with the requests library

# COMMAND ----------

import requests
import pandas as pd
import io

# COMMAND ----------

url = requests.get('https://raw.githubusercontent.com/mattiazenidb/eni-workshop/main/_resources/turbine.csv').content

# COMMAND ----------

raw_data = pd.read_csv(io.StringIO(url.decode('utf-8')))

# COMMAND ----------

df_spark_raw_data = spark.createDataFrame(raw_data)

# COMMAND ----------

df_spark_raw_data.count()

# COMMAND ----------

df_spark_raw_data.display()

# COMMAND ----------

df_spark_raw_data.write.mode('overwrite').option("mergeSchema", "true").saveAsTable(f'{catalog}.{schema}.turbine_intermediate')

# COMMAND ----------

df_turbine_intermediate = spark.read.table(f'{catalog}.{schema}.turbine_intermediate')

# COMMAND ----------

df_turbine_intermediate.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

df_turbine_intermediate = df_turbine_intermediate.withColumn("lat", col("lat").cast("double"))\
                          .withColumn("long", col("long").cast("double"))

# COMMAND ----------

df_turbine_intermediate.printSchema()

# COMMAND ----------

df_turbine_intermediate.write.mode('overwrite').option("mergeSchema", "true").saveAsTable(f'{catalog}.{schema}.turbine')

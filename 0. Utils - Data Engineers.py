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

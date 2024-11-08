# Databricks notebook source
# MAGIC %sql
# MAGIC Create table if not exists users_data

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into users_data
# MAGIC from 'dbfs:/mnt/adlssonydatabricks/raw/sample/'
# MAGIC FILEFORMAT = csv
# MAGIC FORMAT_OPTIONS ('header' = 'true',
# MAGIC                 'mergeSchema'='true')
# MAGIC copy_options ('mergeSchema'='true');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from users_data

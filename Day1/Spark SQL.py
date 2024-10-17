# Databricks notebook source
# MAGIC %sql
# MAGIC --select * from fileformat.`path`

# COMMAND ----------

# DBTITLE 1,Query
# MAGIC %sql
# MAGIC select * from json.`/Volumes/gogul_databricks/default/raw/customers.json`
# MAGIC --here since this is single line json the column datatype are added otherwise there is no option in sql to do so unlike pyspark

# COMMAND ----------

# DBTITLE 1,CTAS
# MAGIC %sql
# MAGIC create table customers_spark_sql as select *,current_timestamp() as ingestion_date from json.`/Volumes/gogul_databricks/default/raw/customers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table product as 
# MAGIC select *,current_timestamp() as ingestion_date from json.`/Volumes/gogul_databricks/default/raw/products.json`

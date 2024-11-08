# Databricks notebook source
# MAGIC %sql
# MAGIC --merge and upsert makes sure duplicates are not there 

# COMMAND ----------

data = [(1, 'Sachin'),(2, 'Virat')]
schema = 'id int, name string'
df = spark.createDataFrame(data,schema)

# COMMAND ----------

df.write.saveAsTable('training.bronze.cricket')

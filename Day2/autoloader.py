# Databricks notebook source
schema = 'Id int, Name string, Gender string, Country string, date string'

# COMMAND ----------

spark.readStream.schema(schema).csv('/Volumes/training/bronze/stream_in/stream/', header=True).writeStream.option("checkpointLocation",'/FileStore/tables/checkpoint').trigger(once=True).table('training.bronze.stream')

# COMMAND ----------

spark.readStream.format('cloudFiles').option("cloudFiles.format",'csv').option('cloudFiles.schemaLocation','/FileStore/tables/schemaLocation').load('/Volumes/training/bronze/stream_in/stream/').writeStream.option('checkpointLocation','/FileStore/tables/checkpoint_autoloader').table('training.bronze.autoloader')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from training.bronze.autoloader

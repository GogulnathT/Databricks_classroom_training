# Databricks notebook source
# batch: 
#     df=spark.read.csv('path')
#     df.write.saveAsTable('tablename')

# stream
#     df = spark.readStream.csv('path')
#     df.writeStream.option('checkpointLocation').table('tablename') # this checkpointLocation is mandatory

# COMMAND ----------

schema = 'Id int, Name string, Gender string, Country string, date string'

# COMMAND ----------

df = spark.read.schema(schema).csv('/Volumes/training/bronze/stream_in/stream/Jan.CSV', inferSchema=True)

# COMMAND ----------

# df = spark.readStream.schema(schema).csv('path') # here the path has to be a directory not a file path; also schema is mandatory for schema 
# dont use display on stream df as it will keep the cluster running forever, increasing the bill
spark.readStream.schema(schema).csv('/Volumes/training/bronze/stream_in/stream/', header=True).writeStream.option("checkpointLocation",'/FileStore/tables/checkpoint').trigger(once=True).table('training.bronze.stream')

# COMMAND ----------

# use trigger to set what causes the write trigger refer spark docs
# providing the trigger is best practice 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from training.bronze.stream

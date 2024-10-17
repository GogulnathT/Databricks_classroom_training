# Databricks notebook source
'''
ways to handle etl 
- pyspark df 
- Spark SQL 

Extract
data format(csv, table, json, parquet, delta, avro, orc)
(ADLS, databases, DW, S3)

Transforms

Load
csv, jsonm parquet, delta
parquet is preferred as it saves storage 
delta is built on top of parquet and it is much safer and efficient 

'''


# COMMAND ----------

path = '/Volumes/gogul_databricks/default/raw/sales.csv'
df_sales = spark.read.csv(path, header=True, inferSchema=True) 
# .csv cause of the file format; eg .parquet, .delta etc 
# header is true to assign columns and inferschema so that the datatype is assigned correctly 
df.display()

# COMMAND ----------

df_customers = spark.read.json('/Volumes/gogul_databricks/default/raw/customers.json')
#for SINGLE LINE json, header and other options do not need to be mentioned it will be picked automatically from the metadata
df_customers.display()

# COMMAND ----------

df_customers.write.saveAsTable('customer') # this will be saved as a delta table in the catalog


# COMMAND ----------


df_sales.write.mode('overwrite').saveAsTable('sales') # this option will overwrite the existing table

# COMMAND ----------



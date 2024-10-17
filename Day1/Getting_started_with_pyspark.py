# Databricks notebook source
print("hello world")

# COMMAND ----------

data = [(1,'a',20),(2,'b',30)]
# schema = ["id", "name", "age"]
schema = "id int, name string, age int"
df = spark.createDataFrame(data,schema)
df.display() # this .display() method is available only on databricks; display(df) will also work only on dbr


# COMMAND ----------

df.show()

# COMMAND ----------

Dataframe functions 

.select
.alias
.withColumnRenamed
.withColumnsRenamed
.withColumn

Functions 
col

# COMMAND ----------

df.select("*") # here spark job is not created as action is not taken i.e display

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

df.select('id','age').display()

# COMMAND ----------

df.select(col('id').alias("emp_id")) # this will throw error as the col func has not been imported 

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col('id').alias("emp_id")).display()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df.withColumnRenamed("id","emp_id").display() # dataframe function

# COMMAND ----------

df_new = df.withColumnsRenamed({'id':'emp_id', 'name':'emp_name', 'age':'emp_age'})
df_new.display()

# COMMAND ----------

df.withColumn("current_date",current_date()).display() # this will create a new column, current_date is a func imported

# COMMAND ----------

df.withColumn("age",current_date()).display() # this will replave the existing column values of age with current date values

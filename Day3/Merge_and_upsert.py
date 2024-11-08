# Databricks notebook source
# MAGIC %sql
# MAGIC --merge and upsert makes sure duplicates are not there 

# COMMAND ----------

data=[(1,'Steve','USA'),(2,'Elon','Canada')]
schema="id int, name string, Country String"
df=spark.createDataFrame(data,schema)

# COMMAND ----------

df.createOrReplaceTempView("billion_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table Billionare(id int, name string, country string)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO Billionare b
# MAGIC USING billion_view bv
# MAGIC ON b.id = bv.id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     b.name = bv.name,
# MAGIC     b.country = bv.country
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     id,
# MAGIC     name,
# MAGIC     country
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     id,
# MAGIC     name,
# MAGIC     country
# MAGIC   )

# COMMAND ----------

data=[(1,'Steve','India'),(3,'Mukesh','India')]
schema="id int, name string, Country String"
df=spark.createDataFrame(data,schema)

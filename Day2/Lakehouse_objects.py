# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog training
# MAGIC create schema bronze; -- create schema if not exists training.bronze;
# MAGIC use bronze; -- this makes sure the table goes to this schema 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table emp(id int, name string, age int)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail training.bronze.emp;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended emp;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into emp values(1, 'a', 30); -- each insertion will create a separate file which can be checked using the details 
# MAGIC insert into emp values(2, 'b', 32);
# MAGIC insert into emp values(3, 'c', 33);
# MAGIC insert into emp values(4, 'd', 28),(5, 'e', 25),(6, 'f', 36); -- here in a multiple record insertions are done on the same command so one file will only be created 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp order by id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- views are of three types. views are virtual tables that dont have size to them. views can be used to analyse a sample of a large dataset;
# MAGIC -- std (this will persist even if we close the notebook)
# MAGIC -- temp views (this will be terminated if the cluster is stopped)
# MAGIC -- global temp views (these are also temporary but they will be available across notebooks)

# COMMAND ----------

# MAGIC %sql
# MAGIC create view emp3 as select * from emp where id>3 
# MAGIC -- this will get saved into the catalog and schema which have been mentioned in this notebook(training and bronze in this case)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating temp view
# MAGIC create temp view emp as select * from emp where id = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp; -- here the temp view has the same name as table, mention schema name to access table instead of view

# COMMAND ----------

# MAGIC %sql
# MAGIC show views 

# COMMAND ----------

df = spark.read.csv('/Volumes/gogul_databricks/default/raw/sales.csv')

# COMMAND ----------

df.createOrReplaceGlobalTempView('sales_global_temp')

# COMMAND ----------

# MAGIC %sql
# MAGIC show views in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_global_temp

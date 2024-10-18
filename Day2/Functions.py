# Databricks notebook source
# MAGIC %sql
# MAGIC -- # create function function_name(para1 datatype)
# MAGIC -- # returns string 
# MAGIC -- # return logic

# COMMAND ----------

# MAGIC %sql
# MAGIC -- this function gets stored in default schema 
# MAGIC Create or replace function sale_announcement(item_name STRING, item_price INT)
# MAGIC returns string
# MAGIC return concat('The ', item_name, ' is on sale for $', round(item_price * 0.2, 0));

# COMMAND ----------



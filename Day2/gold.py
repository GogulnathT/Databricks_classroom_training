# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists gogul_databricks.gold

# COMMAND ----------

# MAGIC %sql
# MAGIC select week_of_year, count(*) as count from gogul_databricks.default.order_dates group by weekofyear
# MAGIC  

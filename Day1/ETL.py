# Databricks notebook source
# MAGIC %run /Workspace/Day1/includes

# COMMAND ----------

df_sales=spark.read.csv(f"{input_path}sales.csv")

# COMMAND ----------

df1 = add_ingestion(df_sales)

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable('order_dates')

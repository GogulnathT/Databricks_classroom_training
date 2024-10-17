# Databricks notebook source


# COMMAND ----------

# MAGIC %run /Workspace/Users/gogul.nath123@outlook.com/Databricks_classroom_training/Day1/includes

# COMMAND ----------

df_sales=spark.read.csv(f"{input_path}sales.csv")

# COMMAND ----------

# calling function
df1 = add_ingestion(df_sales)

# COMMAND ----------

# creating widgets
# dbutils.help()
# dbutils.widgets.help()
dbutils.widgets.text('environment','dev')
dbutils.widgets.get('environment')
# text widgets aka parameters can be created using the ui using Edit->parameters

# COMMAND ----------

v = dbutils.widgets.get('environment')
v

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable('order_dates')

# COMMAND ----------

df2 = df1.withColumn('environment',lit(v))

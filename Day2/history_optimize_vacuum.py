# Databricks notebook source
# MAGIC %sql
# MAGIC select * from gogul_databricks.default.customer

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gogul_databricks.default.customer (customer_city, customer_email, customer_id, customer_name, customer_state) values ('Chennai', 'gogul@example.com', 6, 'Gogulnath', 'TN')

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from gogul_databricks.default.customer where customer_name = 'Gogulnath'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history gogul_databricks.default.customer

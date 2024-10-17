# Databricks notebook source
df_sales = spark.table("sales") # creating df from table(not from file hence .table)

# COMMAND ----------

df_customer = spark.table('customer')

# COMMAND ----------

df_joined = df_sales.join(df_customer, "customer_id",'inner') # customer id from df sales will be taken
# df_joined = df_sales.join(df_customer, df_sales['customer_id']==df_customer["customer_id"],'inner')
# this command creates two customer_id columns which will later cause problem in orderby and groupby, this syntax should be used only when there are different column names 

# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_product = spark.table('product')

# COMMAND ----------

df_joined_2 = df_sales.join(df_product, df_sales['product_id']==df_product['product_id'], 'inner')
df_joined_2.display()

# COMMAND ----------

#filtering 
df_customer.filter('customer_id = 2').display() 

# COMMAND ----------

# instead of .filter, where can also be used
from pyspark.sql.functions import * 
df_customer.where(col('customer_id')==2).display()
# print(type(col('customer_id')==2))

# COMMAND ----------

# here orderby can also be used 
# df_customer.sort('customer_name').display()
df_customer.sort(col('customer_city').desc()).display() 
# df_customer.sort('customer_name', ascending=False).display() ; this can be used instead of above line but not recommended

# COMMAND ----------

df_joined.count()

# COMMAND ----------

df_joined.groupBy('customer_id').count().orderBy("customer_id").display()

# COMMAND ----------



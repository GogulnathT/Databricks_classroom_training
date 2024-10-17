# Databricks notebook source
from pyspark.sql.functions import *
simpleData = ((1,"James", "Sales", 3000), \
    (2,"Michael", "Sales", 4600),  \
    (3,"Robert", "Sales", 4100),   \
    (4,"Maria", "Finance", 3000),  \
    (5,"James", "Sales", 3000),    \
    (6,"Scott", "Finance", 3300),  \
    (7,"Jen", "Finance", 3900),    \
    (8,"Jeff", "Marketing", 3000), \
    (9,"Kumar", "Marketing", 2000),\
    (10,"Saif", "Sales", 4100), \
    (6,"Scott", "Finance", 3300),  \
    (7,"Jen", "Finance", 3900),    \
    (8,"Jeff", "Marketing", 3000), \
    (9,"Kumar", "Marketing", 2000),\
    (10,"Saif", "Sales", 4100),\
    (6,"Scott", "Finance", 3300),  \
    (7,"Jen", "Finance", 3900),    \
    (8,"Jeff", "Marketing", 3000), \
    (9,"Kumar", "Marketing", 2000),\
    (10,"Saif", "Sales", 4100), \
    (None,None,None,None),\
    (None,None,None,None),\
    (None,None,None,None),\
    (None,"Robert",None,2000),\
     (11,"Jack", None, 4100), \
    (12,"Steve", "Sales", None)  
  )
 
columns= ["id","employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.display()

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df1 = df.dropDuplicates(['id'])
df1.display()

# COMMAND ----------

df2 = df1.dropna("all")

# COMMAND ----------

help(df.dropna) # format for getting help on df methods 
help(col) # format for getting help on df functions; the func should be imported

# COMMAND ----------

# df2.fillna("Sales") # this will fill all the null in columns with string data type as 'Sales'
df2.fillna("Sales","department").display()

# COMMAND ----------

df3 = df2.fillna({'department':'Sales', 'salary':4600}) # using dict to replace nulls in multiple columns at the same time
df3.display()

# COMMAND ----------

df3.withColumn('salary_category',when(df3['salary']<3500,'Low').when((df3['salary']>3500) & (df3['salary']<4000), 'Medium').otherwise('High')).display()

# COMMAND ----------

# DBTITLE 1,Using Window
from pyspark.sql.window import *
w = Window.partitionBy('department').orderBy(col('salary').desc())

# COMMAND ----------

df3.withColumn('rank',dense_rank().over(w)).display()

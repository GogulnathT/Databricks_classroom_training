# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.functions import *
import datetime


task_data = [{
	"id": "0001",
	"type": "donut",
	"name": "Cake",
	"ppu": 0.55,
	"batters":
		{
			"batter":
				[
					{ "id": "1001", "type": "Regular" },
					{ "id": "1002", "type": "Chocolate" },
					{ "id": "1003", "type": "Blueberry" },
					{ "id": "1004", "type": "Devil's Food" }
				]
		},
	"topping":
		[
			{ "id": "5001", "type": "None" },
			{ "id": "5002", "type": "Glazed" },
			{ "id": "5005", "type": "Sugar" },
			{ "id": "5007", "type": "Powdered Sugar" },
			{ "id": "5006", "type": "Chocolate with Sprinkles" },
			{ "id": "5003", "type": "Chocolate" },
			{ "id": "5004", "type": "Maple" }
		]
}]

# COMMAND ----------

df = spark.createDataFrame(task_data)
df.display()


# COMMAND ----------

df1 = df.withColumn('topping',explode('topping')).withColumn('topping_type',col("topping.type")).withColumn("topping_id",col("topping.id")).drop('topping')
df1.display()

# COMMAND ----------

final_df = df1.withColumn('batter_type',col("batters.batter")).drop('batters')
display(final_df)


# COMMAND ----------

donut_df = final_df.withColumn('batter_type',explode('batter_type')).withColumn("batter_id",col('batter_type.id')).withColumn('batter_type',col('batter_type.type'))
donut_df.display()

# COMMAND ----------

donut_df.write.saveAsTable("donut_table")

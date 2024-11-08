-- Databricks notebook source
create streaming live table sales_bronze as --creating delta live table CTAS 
select * from cloud_files('/Volumes/training/default/sales', 'csv',map("cloudFiles.inferColumnTypes","True")) --using autoloader 

-- COMMAND ----------

create streaming live table sales_silver
(constraint valid_order_id expect(order_id is not null) on violation drop row
)
as 
select distinct * from STREAM(live.sales_bronze)  

-- COMMAND ----------

create streaming live table customers_bronze as --creating delta live table CTAS 
select * from cloud_files('/Volumes/training/default/customers', 'csv',map("cloudFiles.inferColumnTypes","True")) --using autoloader 

-- COMMAND ----------

create streaming live table product_bronze as --creating delta live table CTAS 
select * from cloud_files('/Volumes/training/default/product', 'csv',map("cloudFiles.inferColumnTypes","True")) --using autoloader 

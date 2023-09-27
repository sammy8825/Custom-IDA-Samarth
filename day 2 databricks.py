# Databricks notebook source
from pyspark.sql.functions  import *

# COMMAND ----------

# dbutils.fs.mount(
#   source = "wasbs://raw@saunextadls.blob.core.windows.net",
#   mount_point = "/mnt/saunextadls/raw",
#   extra_configs = {"fs.azure.account.key.saunextadls.blob.core.windows.net":"DsZWJs7JVVHZz1I7GKyclV8ejCdj0V2UkqMlgAp6QyVOw5rvrHvmVTgwcThdHUymWg7MXon65/0z+AStj4Yiug=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/saunextadls/raw/json/

# COMMAND ----------

df = spark.read.json("dbfs:/mnt/saunextadls/raw/json/")

# COMMAND ----------

display(df)

# COMMAND ----------

df1 = df\
.withColumn("ingestion_date", current_date())\
.withColumn("path", input_file_name())

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create schema if not exists json

# COMMAND ----------

df1.write.mode("overwrite").option("path", "dbfs:/mnt/saunextadls/raw/output/samarth/json").saveAsTable("json.bronzejson")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from json.bronzejson

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from json.bronzejson

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema sample

# COMMAND ----------

# MAGIC %sql
# MAGIC create table emp(id int, name string, age int, dept string)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail emp

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended emp

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history emp

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table emp

# COMMAND ----------

# MAGIC %sql
# MAGIC create table emp(id int, name string, age int, dept string) location "dbfs:/mnt/saunextadls/raw/delta/samarth/emp"

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended emp

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history emp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into table emp values(1,'a',23,'DE')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history emp

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into table emp values(2,'b',23,'DE')

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into table emp values(3,'c',23,'DE'), (4,'d',23,'DE')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history emp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from emp where id= 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

# MAGIC %sql
# MAGIC Update emp
# MAGIC set dept='DS'
# MAGIC where id= 4

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history emp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp version as of 3

# COMMAND ----------

# MAGIC %sql
# MAGIC create table oldemp as 
# MAGIC select * from emp version as of 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp timestamp as of '2023-09-27T08:41:04.000+0000'

# COMMAND ----------



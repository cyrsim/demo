# Databricks notebook source
diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")

# COMMAND ----------

from pyspark.sql.functions import avg
  
display(diamonds.select("color","price").groupBy("color").agg(avg("price")))

# COMMAND ----------

df = spark.read.option("header",True).csv("/databricks-datasets/airlines/part-00000")

# COMMAND ----------

df.write.saveAsTable("airlines")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from airlines 
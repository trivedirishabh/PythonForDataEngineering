# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("csv")\
        .option("header",True)\
        .load("/Volumes/sparkcatalog/raw/source/raw_orders/orders.csv")

# COMMAND ----------

# Wide Transformation
df = df.repartition(2)

# Narrow Transformation - 1
df = df.select("order_id","customer_id")

# Narrow Transformation - 2
df = df.filter(col("order_id")==1001)

# Wide Transformation
df = df.groupBy("customer_id").count()


# COMMAND ----------

display(df)

# COMMAND ----------


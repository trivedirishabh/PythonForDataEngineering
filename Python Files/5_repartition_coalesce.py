# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("csv")\
        .option("header",True)\
        .load("/Volumes/sparkcatalog/raw/source/raw_orders/")

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.withColumn("spark_partition",spark_partition_id())\
    .groupBy("spark_partition")\
    .agg(count("*").alias("count"))\
    .orderBy("spark_partition"))

# COMMAND ----------

df_repart = df.repartition(4)
display(df_repart.withColumn("spark_partition",spark_partition_id())\
    .groupBy("spark_partition")\
    .agg(count("*").alias("count"))\
    .orderBy("spark_partition"))

# COMMAND ----------

df_coal = df_repart.coalesce(2)
display(df_coal.withColumn("spark_partition",spark_partition_id())\
    .groupBy("spark_partition")\
    .agg(count("*").alias("count"))\
    .orderBy("spark_partition"))

# COMMAND ----------


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

# MAGIC %md
# MAGIC ## Query Plans

# COMMAND ----------

# This will show all the query plans
df.explain(True)

# COMMAND ----------

# This will only show you the physical plan
df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Plan For Repartition

# COMMAND ----------

df_repart = df.repartition(4)
df_repart.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Query Plan For Coalesce

# COMMAND ----------

df_coal = df.coalesce(2)
df_coal.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Plan For Aggregate

# COMMAND ----------

df_grp = df.withColumn("spark_partition",spark_partition_id())\
    .groupBy("spark_partition")\
    .agg(count("*").alias("count"))\
    .orderBy("spark_partition")

df_grp.explain()

# COMMAND ----------


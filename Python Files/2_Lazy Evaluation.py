# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Lazy Evaluation

# COMMAND ----------

data = [
    (1,"lisa","lisa@random.com"),
    (2,"john","john@random.com"),
    (3,"mary","mary@random.com"),
    (4,"peter","peter@random.com"),
    (5,"jane","jane@random.com"),
    (6,"jim","jim@random.com"),
    (7,"sara","sara@random.com"),
    (8,"bob","bob@random.com"),
    (9,"alex","alex@random.com")
]

columns = ["id","name","email"]

df_new = spark.createDataFrame(data,columns)

# Transformation - 1
df_new = df_new.select("id","email")

# Transformation - 2
df_new = df_new.filter(col("id") > 5)

# COMMAND ----------

display(df_new)

# COMMAND ----------


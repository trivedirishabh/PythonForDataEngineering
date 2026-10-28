# Databricks notebook source
import pyspark.pandas as ps
import pandas as pd

# COMMAND ----------

df = ps.read_csv("/Volumes/sparkcatalog/raw/source/raw_orders/orders.csv")
df.head()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.describe()

# COMMAND ----------

df[['order_id','customer_id','price']].head()

# COMMAND ----------

df['new_col'] = df['price']*10
df.head()

# COMMAND ----------

df['flag'] = df['price']>1000
df.head()

# COMMAND ----------

df[df['price']>10].head()

# COMMAND ----------

df.sort_values('price',ascending=False).head()

# COMMAND ----------

# MAGIC %md
# MAGIC # Pandas UDF

# COMMAND ----------

# MAGIC %md
# MAGIC ### Series To Series

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

@pandas_udf("double")
def price_func(col1:pd.Series,col2:pd.Series) -> pd.Series:
    return col1 * col2

# COMMAND ----------

# Creating a PySpark Dataframe
df_spark = spark.read.format("csv")\
                .option("header","true")\
                .option("inferSchema","true")\
                .load("/Volumes/sparkcatalog/raw/source/raw_orders/orders.csv")


df_spark = df_spark.withColumn("total_price",price_func('price','quantity'))

display(df_spark)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Iterator Series To Iterator Series

# COMMAND ----------

from typing import Iterator, Tuple

@pandas_udf("double")
def iter_func(col1: Iterator[pd.Series]) -> Iterator[pd.Series]:

    for batch in col1:
        yield batch * 10

  

# COMMAND ----------

df_spark = df_spark.withColumn("iter_price",iter_func('price'))
display(df_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Multiple Iterator Series To Iterator Series

# COMMAND ----------

@pandas_udf("double")
def price_iter_mul(cols: Iterator[Tuple[pd.Series, pd.Series]]) -> Iterator[pd.Series]:
    
    for col1, col2 in cols:
        yield col1 * col2



# COMMAND ----------

df_spark = df_spark.withColumn("iter_price_mul",price_iter_mul('price','quantity'))
display(df_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Series To Scalar

# COMMAND ----------

@pandas_udf("double")
def scalar_func(col1: pd.Series) -> float:
  return col1.mean()
  

# COMMAND ----------

df_spark = df_spark.groupBy("customer_id").agg(scalar_func('price').alias("mean_price"))
display(df_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using It With Spark SQL

# COMMAND ----------

df_spark = spark.read.format("csv")\
                .option("header","true")\
                .option("inferSchema","true")\
                .load("/Volumes/sparkcatalog/raw/source/raw_orders/orders.csv")

# COMMAND ----------

df_spark.createOrReplaceTempView("df_spark")

# COMMAND ----------

spark.udf.register("price_iter_mul_udf", price_iter_mul)

# COMMAND ----------

display(spark.sql("""
          SELECT 
            *,
            price_iter_mul_udf(price, quantity) as iter_price_mul
        FROM 
            df_spark"""))

# COMMAND ----------


# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA READING

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading CSVs

# COMMAND ----------

df_csv = spark.read.format("csv")\
      .option("header",True)\
      .option("inferSchema",True)\
      .load("/Volumes/sparkcatalog/raw/source/raw_orders/orders.csv")

display(df_csv)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading JSONs

# COMMAND ----------

df_json = spark.read.format("JSON")\
        .option("inferSchema",True)\
        .option("multiLine",True)\
        .load("/Volumes/sparkcatalog/raw/source/raw_orders/orders.json")

display(df_json)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Parquet

# COMMAND ----------

df_parquet = spark.read.format("parquet")\
        .load("/Volumes/sparkcatalog/raw/source/raw_orders/part-00000-tid-orders.c000.snappy.parquet")

display(df_parquet)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading JDBC

# COMMAND ----------

# my_url = "jdbc:postgresql://localhost:5432/mydb"

# # Option - 1 To Create DF

# my_connection = {
#   "user": "postgres",
#   "password": "postgres",
#   "driver": "org.postgresql.Driver"
# }

# df = spark.read.jdbc(url = my_url, table = "orders", properties = my_connection)




# # Option - 2 To Create DF
# df = spark.read.format("jdbc")\
#           .option("url", my_url)\
#           .option("dbtable", "mydb.orders")\
#           .option("user", "postgres")\
#           .option("password", "postgres")\
#           .option("driver", "org.postgresql.Driver")\
#           .load()




# COMMAND ----------

# MAGIC %md
# MAGIC # CORRUPT RECORDS MODES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Permissive

# COMMAND ----------

df_json = spark.read.format("JSON")\
        .option("inferSchema",True)\
        .option("multiLine",True)\
        .option("mode","PERMISSIVE")\
        .load("/Volumes/sparkcatalog/raw/source/raw_orders/orders.json")

display(df_json)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROPMALFORMED

# COMMAND ----------

df_json = spark.read.format("JSON")\
        .option("inferSchema",True)\
        .option("multiLine",True)\
        .option("mode","DROPMALFORMED")\
        .load("/Volumes/sparkcatalog/raw/source/raw_orders/orders.json")

display(df_json)

# COMMAND ----------

# MAGIC %md
# MAGIC ### FAILFAST

# COMMAND ----------

df_json = spark.read.format("JSON")\
        .option("inferSchema",True)\
        .option("multiLine",True)\
        .option("mode","FAILFAST")\
        .load("/Volumes/sparkcatalog/raw/source/raw_orders/orders.json")

display(df_json)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # Schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### StructType

# COMMAND ----------

df_csv.schema

# COMMAND ----------

my_custom_schema = StructType([StructField('order_id', StringType(), True), StructField('customer_id', StringType(), True), StructField('order_date', DateType(), True), StructField('product_id', StringType(), True), StructField('quantity', IntegerType(), True), StructField('price', DoubleType(), True), StructField('order_status', StringType(), True), StructField('shipping_address', StringType(), True), StructField('city', StringType(), True), StructField('country', StringType(), True), StructField('payment_method', StringType(), True), StructField('discount', DoubleType(), True), StructField('category', StringType(), True), StructField('sales_rep', StringType(), True), StructField('region', StringType(), True), StructField('ship_date', DateType(), True), StructField('delivery_days', IntegerType(), True), StructField('returned', StringType(), True), StructField('gender', StringType(), True)])

df_csv_custom = spark.read.format("csv")\
      .option("header",True)\
      .schema(my_custom_schema)\
      .load("/Volumes/sparkcatalog/raw/source/raw_orders/orders.csv")

display(df_csv_custom)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL Schema

# COMMAND ----------

my_ddl_schema = """
  order_id INT,
  customer_id STRING,
  order_date DATE,
  product_id STRING,
  quantity INT,
  price DOUBLE,
  order_status STRING,
  shipping_address STRING,
  city STRING,
  country STRING,
  payment_method STRING,
  discount DOUBLE,
  category STRING,
  sales_rep STRING,
  region STRING,
  ship_date DATE,
  delivery_days INT,
  returned STRING,
  gender STRING
  """

df_csv_ddl = spark.read.format("csv")\
      .option("header",True)\
      .schema(my_ddl_schema)\
      .load("/Volumes/sparkcatalog/raw/source/raw_orders/orders.csv")

display(df_csv_ddl)

# COMMAND ----------

# MAGIC %md
# MAGIC # SELECT

# COMMAND ----------

df_select = df_csv.select('customer_id',col('city'),'country')
display(df_select)

# COMMAND ----------

# MAGIC %md
# MAGIC # ALIAS

# COMMAND ----------

df_alias = df_csv.select('customer_id',col('city').alias('customer_city'),col('country').alias('customer_country'))

display(df_alias)

# COMMAND ----------

# MAGIC %md
# MAGIC # FILTER

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Scenario-1

# COMMAND ----------

df_filter1 = df_csv.filter(col('order_status')=='Returned')
display(df_filter1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-2

# COMMAND ----------

df_filter2 = df_csv.filter((col('order_status')=='Returned') | (col('order_status') == 'Cancelled'))
display(df_filter2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-3
# MAGIC

# COMMAND ----------

desired_order = ['Returned','Cancelled','Shipped']

df_filter3 = df_csv.filter(col('order_status').isin(desired_order))
display(df_filter3)

# COMMAND ----------

# MAGIC %md
# MAGIC # withColumnRenamed

# COMMAND ----------

df_renamed = df_csv.withColumnRenamed('order_status','order_status_info')
display(df_renamed)


# COMMAND ----------

# MAGIC %md
# MAGIC # withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-1
# MAGIC

# COMMAND ----------

df_mod1 = df_csv.withColumn('flag',lit(0))
display(df_mod1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-2

# COMMAND ----------

df_mod2 = df_csv.withColumn('shipping_address',regexp_replace('shipping_address',',.*',''))
display(df_mod2)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-3

# COMMAND ----------

df_mod3 = df_csv.withColumn("total_price",col('price')*col('quantity'))\
            .withColumn("total_price",round(col('total_price'),2))
display(df_mod3)

# COMMAND ----------

# MAGIC %md
# MAGIC # TYPE CASTING

# COMMAND ----------

df_cast1 = df_csv.withColumn("order_id",col('order_id').cast(StringType()))
display(df_cast1)

# COMMAND ----------

df_cast2 = df_csv.withColumn("order_id",col('order_id').cast('STRING'))
display(df_cast2)

# COMMAND ----------

# MAGIC %md
# MAGIC # SORTING

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-1

# COMMAND ----------

df_sort1 = df_csv.sort(col('order_date').desc())
display(df_sort1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-2

# COMMAND ----------

df_sort2 = df_csv.sort(['order_date','quantity'],ascending=[0,1])
display(df_sort2)

# COMMAND ----------

# MAGIC %md
# MAGIC # LIMIT

# COMMAND ----------

display(df_csv.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC # DROP

# COMMAND ----------

df_drop = df_csv.drop('order_status','shipping_address')
display(df_drop)

# COMMAND ----------

# MAGIC %md
# MAGIC # DROP DUPLICATES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-1

# COMMAND ----------

df_dedups = df_csv.dropDuplicates()
display(df_dedups)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-2

# COMMAND ----------

df_dedups2 = df_csv.dropDuplicates(subset=['order_date','product_id'])
display(df_dedups2)

# COMMAND ----------

# MAGIC %md
# MAGIC # Union & UnionByName

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union

# COMMAND ----------

data1 = [(1,'aa','123'),(2,'bb','456'),(3,'cc','789')]
df1 = spark.createDataFrame(data1,['id','name','address'])
data2 = [(4,'aa','123'),(5,'bb','456'),(6,'cc','789'),(7,'dd','123')]
df2 = spark.createDataFrame(data2,['id','name','address'])

df_union = df1.union(df2)
display(df_union)

# COMMAND ----------

# MAGIC %md
# MAGIC ### UnionByName

# COMMAND ----------

data1 = [(1,'aa','123'),(2,'bb','456'),(3,'cc','789')]
df1 = spark.createDataFrame(data1,['id','name','address'])
data2 = [('123','aa',4),('456','bb',5),('789','cc',6),('123','dd',7)]
df2 = spark.createDataFrame(data2,['address','name','id'])

df_union = df1.unionByName(df2)
display(df_union)

# COMMAND ----------

# MAGIC %md
# MAGIC # DATE FUNCTIONS

# COMMAND ----------

df_curr = df_csv.withColumn("current_time",current_timestamp())
display(df_curr)

# COMMAND ----------

df_add = df_curr.withColumn("current_time",date_add('current_time',7))
display(df_add)

# COMMAND ----------

df_sub = df_curr.withColumn("current_time",date_sub('current_time',7))
display(df_sub)


# COMMAND ----------

df_diff = df_curr.withColumn("duration",date_diff('current_time','order_date'))
display(df_diff)

# COMMAND ----------

df_format = df_curr.withColumn("current_time",date_format('current_time','yyyy-MM-dd'))
display(df_format)


# COMMAND ----------

# MAGIC %md
# MAGIC # STRING FUNCTIONS

# COMMAND ----------

df_str = df_csv.withColumn("order_status",lower('order_status'))
display(df_str)

# COMMAND ----------

df_str = df_csv.withColumn("order_status_length",length('order_status'))
display(df_str)

# COMMAND ----------

# MAGIC %md
# MAGIC # Handling Nulls

# COMMAND ----------

data = [
    (1, 'abc', None),
    (2, 'def', 'xyz'),
    (3, 'ghi', 'xyz'),
    (4, 'jkl', 'zyx'),
    (None, None, None),
    (6, 'pqr', 'bbb'),
    (7, 'stu', 'ihd'),
    (8, 'vwx', None),
    (9, 'yz', None)
]

df = spark.createDataFrame(
    data,
    ['id', 'name', 'address']
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### All Nulls

# COMMAND ----------

df_all_nulls = df.dropna('all')
display(df_all_nulls)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Any Null

# COMMAND ----------

df_any_null = df.dropna('any')
display(df_any_null)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Subset

# COMMAND ----------

df_subset_null = df.dropna(subset=['name','address'],how='all')
display(df_subset_null)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fill Nulls

# COMMAND ----------

df_fillna = df.fillna('dummy')
display(df_fillna)

# COMMAND ----------

df_custom_fillna = df.fillna({'name':'dummy_name','address':'dummy_address'})
display(df_custom_fillna)

# COMMAND ----------

# MAGIC %md
# MAGIC # Split & Indexing

# COMMAND ----------

df_split = df_csv.withColumn("street_address",split('shipping_address',',')[0])\
                .withColumn("city_name",split('shipping_address',',')[1])

display(df_split)

# COMMAND ----------

# MAGIC %md
# MAGIC # EXPLODE

# COMMAND ----------

df_arr = df_csv.withColumn("address_array",split("shipping_address",","))\
                .select("order_id","address_array")       
display(df_arr)

# COMMAND ----------

df_exp = df_arr.withColumn("address_explode",explode("address_array"))
display(df_exp)

# COMMAND ----------

df_exp = df_arr.withColumn("address_explode",explode_outer("address_array"))
display(df_exp)

# COMMAND ----------

# MAGIC %md
# MAGIC # Array Contains

# COMMAND ----------

df_arr_con = df_arr.withColumn("city_1_flag",array_contains("address_array"," City1"))
display(df_arr_con)

# COMMAND ----------

# MAGIC %md
# MAGIC # Group By

# COMMAND ----------

df_agg = df_csv.withColumn("total_price",col('price')*col('quantity'))\
            .groupBy('product_id').agg(sum('total_price').alias('total_price'))\
            .withColumn("total_price",round(col('total_price'),2))\
            .sort(col('total_price').desc())


display(df_agg)

# COMMAND ----------

df_agg_new = df_csv.withColumn("total_price",col('price')*col('quantity'))\
            .groupBy('product_id','customer_id').agg(sum('total_price').alias('total_price'),avg('total_price').alias('avg_price'))\
            .withColumn("total_price",round(col('total_price'),2))\
            .withColumn("avg_price",round(col('avg_price'),2))\
            .sort('product_id','total_price',ascending=[True,False])

display(df_agg_new)
            

# COMMAND ----------

# MAGIC %md
# MAGIC # **Approx Count**

# COMMAND ----------

df = df_csv.groupBy('product_id').agg(approx_count_distinct('customer_id').alias('distinct'))
display(df)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Collect List

# COMMAND ----------

df_collect = df_csv.groupBy('customer_id').agg(collect_list('product_id').alias('products'))
display(df_collect)

# COMMAND ----------

# MAGIC %md
# MAGIC # PIVOT

# COMMAND ----------

df_pivot = df_csv.groupBy('customer_id').pivot('order_status').agg(count('order_id'))
display(df_pivot)


# COMMAND ----------

# MAGIC %md
# MAGIC # When-Otherwise

# COMMAND ----------

df_ifelse = df_pivot.withColumn('return_flag',when(col('Returned')==1,'low').when(col('Returned')<=3,'mid').when(col('Returned')>3,'high').otherwise('no returns'))

display(df_ifelse)

# COMMAND ----------

df_ifelse_new = df_pivot.withColumn('return_flag',when(col('Returned')==1,'low').when(((col('Returned')==3) & (col('Returned')==2)) ,'mid').when(col('Returned')>3,'high').otherwise('no returns'))

display(df_ifelse_new)

# COMMAND ----------

# MAGIC %md
# MAGIC # JOINS

# COMMAND ----------

data1 = [(1,'abc',901),(2,'def',902),(3,'ghi',903),(4,'jkl',904),(5,'mno',905),(6,'pqr',906),(7,'stu',907),(8,'vwx',908),(9,'tuv',909),(10,'wxy',910),(11,'zab',911),(12,'bcd',912),(13,'efg',913),(14,'hij',914),(15,'klm',915),(16,'nop',916),(17,'qrs',917),(18,'tuv',918),(19,'wxy',919),(20,'zab',920),(21,'bcd',921),(22,'efg',922),(23,'hij',923),(24,'klm',924),(25,'nop',925)]

df1 = spark.createDataFrame(data1,['id','name','code'])

data2 = [(101,1),(102,2),(103,3),(104,4),(105,5),(106,6),(107,7),(108,8),(109,9),(110,10),(111,11),(112,12),(113,13),(114,14),(115,15),(116,31),(117,32),(118,33),(119,34),(120,35),(121,36),(122,37),(123,38),(124,39),(125,40)]
df2 = spark.createDataFrame(data2,['o_id','id'])

display(df1)
display(df2)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner Join

# COMMAND ----------

df_inner = df1.join(df2,df1['id']==df2['id'],'inner')
display(df_inner)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left Join

# COMMAND ----------

df_inner = df1.join(df2,df1['id']==df2['id'],'left')
display(df_left)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right Join

# COMMAND ----------

df_right = df1.join(df2,df1['id']==df2['id'],'right')
display(df_right)

# COMMAND ----------

df_right = df2.join(df1,df1['id']==df2['id'],'left')
display(df_inner)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Join

# COMMAND ----------

df_full = df1.join(df2,df1['id']==df2['id'],'full')
display(df_full)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti Join

# COMMAND ----------

df_anti = df1.join(df2,df1['id']==df2['id'],'anti')
display(df_anti)

# COMMAND ----------

# MAGIC %md
# MAGIC # WINDOW FUNCTIONS

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

data = [(33),(45),(67),(99),(99),(100)]

df = spark.createDataFrame(data,"marks INT")
display(df)

# COMMAND ----------

df_row = df.withColumn("marks_sequence",row_number().over(Window.orderBy('marks')))
display(df_row)

# COMMAND ----------

df_row = df.withColumn("marks_sequence",row_number().over(Window.partitionBy('marks').orderBy('marks')))
display(df_row)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rank VS Dense_Rank

# COMMAND ----------

df_ranking = df.withColumn("rank",rank().over(Window.orderBy('marks')))\
                .withColumn("dense_rank",dense_rank().over(Window.orderBy('marks')))

display(df_ranking)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total Revenue

# COMMAND ----------

data = [(2020,100),(2021,110),(2022,140),(2023,90),(2024,150)]

df = spark.createDataFrame(data,"year INT,revenue INT")
display(df)


# COMMAND ----------

df_total = df.withColumn("total_revenue",sum('revenue').over(Window.orderBy('year')))

display(df_total)

# COMMAND ----------

df_total = df.withColumn("total_revenue",sum('revenue').over(Window.orderBy('year').rowsBetween(Window.unboundedPreceding,Window.currentRow)))

display(df_total)

# COMMAND ----------

df_total = df.withColumn("total_revenue",sum('revenue').over(Window.orderBy('year').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)))

display(df_total)

# COMMAND ----------

# MAGIC %md
# MAGIC # User Defined Functions
# MAGIC

# COMMAND ----------

def my_square(x):
    square = x*x
    return square

# COMMAND ----------

my_square_udf = udf(my_square,FloatType())

# COMMAND ----------

df_cust = df_csv.withColumn("price_sqaure",my_square_udf('price'))
display(df_cust)

# COMMAND ----------

@udf(returnType=FloatType())
def my_square_new(x):
    square = x*x
    return square

# COMMAND ----------

df_cust = df_csv.withColumn("price_sqaure",my_square_new('price'))
display(df_cust)

# COMMAND ----------

# MAGIC %md
# MAGIC # User Defined Table Functions (UDTF)

# COMMAND ----------

df_demo = spark.createDataFrame([("Hello World",), ("Just an example",), ("Hi Bro",)], ["text"])
display(df_demo)

# COMMAND ----------

@udtf(returnType="word STRING")
class udtf_class:
    def eval(self, text: str): # Function name to be used
        for word in text.split():
            yield (word,)

# COMMAND ----------

udtf_class(lit("Hello world, this is a developer")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Call UDF

# COMMAND ----------

spark.udf.register("my_square_new", my_square_new)

df_cust_new = df_csv.withColumn("price_sqaure_new",call_udf("my_square_new",col('price')))
display(df_cust_new)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Concat & ConcatWS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Concat

# COMMAND ----------

df_con = df_csv.withColumn("custom_id",concat(col('customer_id'),lit('|'),col('order_id'),lit('|'),col('product_id')))

display(df_con)

# COMMAND ----------

df_con = df_csv.withColumn("custom_id",concat_ws(lit('|'),col('customer_id'),col('order_id'),col('product_id')))

display(df_con)

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA WRITING

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overwrite

# COMMAND ----------

df_csv.write.format("csv")\
        .mode("overwrite")\
        .option("path","/Volumes/sparkcatalog/raw/source/destination/csv_overwrite")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Append

# COMMAND ----------

df_csv.write.format("csv")\
        .mode("append")\
        .option("path","/Volumes/sparkcatalog/raw/source/destination/csv_overwrite")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Error

# COMMAND ----------

# df_csv.write.format("csv")\
#         .mode("error")\
#         .option("path","/Volumes/sparkcatalog/raw/source/destination/csv_overwrite")\
#         .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ignore

# COMMAND ----------

df_csv.write.format("csv")\
        .mode("ignore")\
        .option("path","/Volumes/sparkcatalog/raw/source/destination/csv_overwrite")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # File Formats

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV

# COMMAND ----------

df_csv.write.format("csv")\
        .mode("append")\
        .option("path","/Volumes/sparkcatalog/raw/source/destination/csv")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### JSON

# COMMAND ----------

df_csv.write.format("json")\
        .mode("append")\
        .option("path","/Volumes/sparkcatalog/raw/source/destination/json")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parquet
# MAGIC

# COMMAND ----------

df_csv.write.format("parquet")\
        .mode("append")\
        .option("path","/Volumes/sparkcatalog/raw/source/destination/parquet")\
        .save()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Delta

# COMMAND ----------

df_csv.write.format("delta")\
        .mode("append")\
        .option("path","/Volumes/sparkcatalog/raw/source/destination/delta")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # UPSERT With Delta Library

# COMMAND ----------

df_new = df_csv.filter(col('order_id').isin(1001,1002))
df_new_1 = df_new.filter(col('order_id')==1001).withColumn("product_id",lit("P102"))
df_new_2 = df_new.filter(col('order_id')==1002).withColumn("order_id",lit("90001"))

df_new = df_new_1.union(df_new_2)
display(df_new)

# COMMAND ----------

from delta.tables import DeltaTable

dlt_obj = DeltaTable.forPath(spark,"/Volumes/sparkcatalog/raw/source/destination/delta/")

dlt_obj.alias("trg").merge(df_new.alias("src"),"trg.order_id = src.order_id")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

# COMMAND ----------

df_test = spark.read.format("delta")\
            .load("/Volumes/sparkcatalog/raw/source/destination/delta")
display(df_test)

# COMMAND ----------

# MAGIC %md
# MAGIC # Handling JSON Data

# COMMAND ----------

df_json = spark.read.format("json")\
                .option("inferSchema",True)\
                .option("multiLine",True)\
                .load("/Volumes/sparkcatalog/raw/source/json_orders/")

# Explode items
df_json = df_json.withColumn("items",explode(col("items")))

# Fetching cols 
df_json = df_json.withColumn("item_id",col("items.item_id"))\
                 .withColumn("quantity",col("items.quantity"))\
                  .withColumn("price",col("items.price"))\
                .withColumn("product_name",col("items.product_name"))\
                .drop("items")\
                .withColumn("customer_id",col("customer.customer_id"))\
                .withColumn("email",col("customer.email"))\
                .withColumn("name",col("customer.name"))\
                .withColumn("city",col("customer.address.city"))\
                .drop("customer")\
                .withColumn("method",col("payment.method"))\
                .withColumn("transaction_id",col("payment.transaction_id"))\
                .drop("payment","metadata")
                


display(df_json)

# COMMAND ----------

# MAGIC %md 
# MAGIC # SparkSQL 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Temporary View

# COMMAND ----------

df_csv.createOrReplaceTempView("csv_view")

# COMMAND ----------

display(spark.sql("SELECT * FROM csv_view"))

# COMMAND ----------

df_sql = spark.sql("""
          SELECT * FROM csv_view
          WHERE order_id IN (1001,1002,90001)""")

# COMMAND ----------

display(df_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Global Temp View

# COMMAND ----------

df_csv.createOrReplaceGlobalTempView("csv_view_global")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS sparkcatalog.raw.new_table")

# COMMAND ----------

spark.sql("""
          CREATE TABLE sparkcatalog.raw.schema_table
          (
                ID INT,
                NAME STRING,
                AGE INT
          )
          """)

# COMMAND ----------

spark.sql(
    """
    INSERT INTO sparkcatalog.raw.schema_table
    VALUES
    (1, 'John', 25),
    (2, 'Jane', 30),
    (3, 'Bob', 35)
    """
)

# COMMAND ----------

display(spark.sql("SELECT * FROM sparkcatalog.raw.schema_table"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### JOINS

# COMMAND ----------

spark.sql("""
          CREATE TABLE sparkcatalog.raw.schema_table_2
          (
                ID INT,
                ADDRESS STRING
          )
          """)

spark.sql("""
            INSERT INTO sparkcatalog.raw.schema_table_2
            VALUES
            (1, '123 Main St'),
            (2, '456 Oak Ave'),
            (3, '789 Pine Ln')
          """)

# COMMAND ----------

display(spark.sql("""
        SELECT * 
        FROM 
            sparkcatalog.raw.schema_table 
            LEFT JOIN 
                sparkcatalog.raw.schema_table_2
                ON 
                    schema_table.ID = schema_table_2.ID
          """))

# COMMAND ----------

# MAGIC %md
# MAGIC ### UPSERT

# COMMAND ----------

data = [(1, 'John', 25), (2, 'Jane', 33),(4, 'Bobi', 35)]
df_new = spark.createDataFrame(data,"id int, name string, age int")

display(df_new)

# COMMAND ----------

df_new.createOrReplaceTempView("new_data")

# Applying Upsert With Merge Command
spark.sql("""
          MERGE INTO sparkcatalog.raw.schema_table t
          USING new_data s
          ON t.ID = s.ID
          WHEN MATCHED THEN
            UPDATE SET *
            WHEN NOT MATCHED
            THEN INSERT *
          """)
display(spark.sql("SELECT * FROM sparkcatalog.raw.schema_table"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### MergeInto API (Latest Spark 4.0+)

# COMMAND ----------

data = [(1, 'John', 25), (2, 'Jane', 36),(4, 'Rob', 35)]
df_new_2 = spark.createDataFrame(data,"id int, name string, age int")

display(df_new_2)

# COMMAND ----------

df_new.alias('src').mergeInto("sparkcatalog.raw.schema_table", col("src.ID") == col("sparkcatalog.raw.schema_table.ID"))\
        .whenMatched().updateAll()\
        .whenNotMatched().insertAll()\
        .merge()

# COMMAND ----------

display(spark.sql("SELECT * FROM sparkcatalog.raw.schema_table"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partition BY

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sparkcatalog.raw.part_table
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY (id)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sparkcatalog.raw.part_table
# MAGIC VALUES (1, 'a'), (2, 'b'), (3, 'c');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE sparkcatalog.raw.part_table;
# MAGIC
# MAGIC DROP TABLE sparkcatalog.raw.part_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXPLAIN

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN SELECT * FROM sparkcatalog.raw.part_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL Functions
# MAGIC You can use all of the SQL functions as-it-is here in SparkSQL as well

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sparkcatalog.raw.part_table

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC WITH inner_query AS (
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     CASE 
# MAGIC     WHEN mod(id, 2) = 0 THEN 'even'
# MAGIC     ELSE 'odd'
# MAGIC     END AS even_odd_flag
# MAGIC   FROM
# MAGIC     sparkcatalog.raw.part_table 
# MAGIC   )
# MAGIC SELECT * FROM inner_query
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # SQL Scripting Statements

# COMMAND ----------

# MAGIC %md
# MAGIC ### For

# COMMAND ----------

# MAGIC %sql
# MAGIC BEGIN
# MAGIC     DECLARE sum INT DEFAULT 0;
# MAGIC     sumNumbers: FOR row AS SELECT num FROM range(1, 20) AS t(num) DO
# MAGIC       IF num > 10 THEN
# MAGIC          LEAVE sumNumbers;
# MAGIC       ELSEIF num % 2 = 0 THEN
# MAGIC         ITERATE sumNumbers;
# MAGIC       END IF;
# MAGIC       SET sum = sum + row.num;
# MAGIC     END FOR sumNumbers;
# MAGIC     VALUES (sum);
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ### If

# COMMAND ----------

# MAGIC %sql
# MAGIC BEGIN
# MAGIC     DECLARE choice DOUBLE DEFAULT 3.9;
# MAGIC     DECLARE result STRING;
# MAGIC     IF choice < 2 THEN
# MAGIC       VALUES ('one fish');
# MAGIC     ELSEIF choice < 3 THEN
# MAGIC       VALUES ('two fish');
# MAGIC     ELSEIF choice < 4 THEN
# MAGIC       VALUES ('red fish');
# MAGIC     ELSEIF choice < 5 OR choice IS NULL THEN
# MAGIC       VALUES ('blue fish');
# MAGIC     ELSE
# MAGIC       VALUES ('no fish');
# MAGIC     END IF;
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Case

# COMMAND ----------

# MAGIC %sql
# MAGIC BEGIN
# MAGIC     DECLARE choice INT DEFAULT 3;
# MAGIC     DECLARE result STRING;
# MAGIC     CASE choice
# MAGIC       WHEN 1 THEN
# MAGIC         VALUES ('one fish');
# MAGIC       WHEN 2 THEN
# MAGIC         VALUES ('two fish');
# MAGIC       WHEN 3 THEN
# MAGIC         VALUES ('red fish');
# MAGIC       WHEN 4 THEN
# MAGIC         VALUES ('blue fish');
# MAGIC       ELSE
# MAGIC         VALUES ('no fish');
# MAGIC     END CASE;
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ### While
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC BEGIN
# MAGIC     DECLARE sum INT DEFAULT 0;
# MAGIC     DECLARE num INT DEFAULT 0;
# MAGIC     sumNumbers: WHILE num < 10 DO
# MAGIC       SET num = num + 1;
# MAGIC       IF num % 2 = 0 THEN
# MAGIC         ITERATE sumNumbers;
# MAGIC       END IF;
# MAGIC       SET sum = sum + num;
# MAGIC     END WHILE sumNumbers;
# MAGIC     VALUES (sum);
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC # SPARKSQL Auxiliary Statements

# COMMAND ----------

# MAGIC %md
# MAGIC ### DESCRIBE

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE sparkcatalog.raw

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE sparkcatalog.raw.part_table

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE QUERY SELECT * FROM sparkcatalog.raw.part_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### REFRESH 

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE sparkcatalog.raw.part_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### SHOW

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS FROM sparkcatalog

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES FROM sparkcatalog.raw

# COMMAND ----------

# MAGIC %sql 
# MAGIC USE sparkcatalog.raw;
# MAGIC SHOW TABLE EXTENDED LIKE 'part_table'

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW TBLPROPERTIES sparkcatalog.raw.part_table

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW PARTITIONS sparkcatalog.raw.part_table

# COMMAND ----------

# MAGIC %md
# MAGIC # SPARKSQL Advanced Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregate Functions

# COMMAND ----------

df_csv.createOrReplaceTempView("sql_tbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sql_tbl

# COMMAND ----------

# pySpark Way
display(df_csv.groupBy("customer_id").agg(array_agg("order_id")))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, array_agg(order_id) FROM sql_tbl GROUP BY customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   collect_list(product_id),
# MAGIC   collect_set(product_id) 
# MAGIC FROM sql_tbl
# MAGIC GROUP BY customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     corr(price,quantity),
# MAGIC     max(price),
# MAGIC     avg(price),
# MAGIC     min(price),
# MAGIC     median(price),
# MAGIC     mode(price)
# MAGIC FROM 
# MAGIC   sql_tbl

# COMMAND ----------

# MAGIC %md
# MAGIC ### STRUCT & MAP

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT struct(1,2,3,"abc")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  named_struct("a",1,"b",2,"c",3)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  map("a",1,"b",2,"c",3)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT map_values(map("a", 1, "b", 2, "c", 3));
# MAGIC
# MAGIC
# MAGIC SELECT map_keys(map("a", 1, "b", 2, "c", 3));

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATETIME FUNCTIONS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_timestamp();
# MAGIC
# MAGIC SELECT add_months(current_timestamp(), 1);
# MAGIC
# MAGIC SELECT add_months(current_timestamp(), -1);
# MAGIC
# MAGIC SELECT convert_timezone('America/Los_Angeles', current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT unix_timestamp()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_from_unix_date(0);
# MAGIC
# MAGIC SELECT from_unixtime(0);
# MAGIC
# MAGIC SELECT from_utc_timestamp(current_timestamp(), 'America/Los_Angeles')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dayname(current_timestamp()),
# MAGIC       dayofmonth(current_timestamp()),
# MAGIC       dayofweek(current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     extract(year from current_timestamp()),
# MAGIC     extract(month from current_timestamp()),
# MAGIC     extract(day from current_timestamp()),
# MAGIC     extract(hour from current_timestamp()),
# MAGIC     extract(minute from current_timestamp()),
# MAGIC     extract(second from current_timestamp()),
# MAGIC     extract(doy from current_timestamp())
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   order_id,
# MAGIC   LAG(order_date, 1, '1900-01-01') OVER(order by order_id) AS prev_order_date,
# MAGIC   order_date,
# MAGIC   LEAD(order_date, 1, '9999-12-31') OVER(order by order_id) AS next_order_date
# MAGIC FROM 
# MAGIC   sql_tbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     order_status,
# MAGIC     ntile(3) OVER(PARTITION BY order_status ORDER BY order_id) AS order_group
# MAGIC FROM
# MAGIC   sql_tbl

# COMMAND ----------

# MAGIC %md
# MAGIC ### Array Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT array(1,2,3);
# MAGIC
# MAGIC SELECT array_append(array(1,2,3),9);
# MAGIC
# MAGIC SELECT array_distinct(array(1,2,3,3));
# MAGIC
# MAGIC SELECT array_compact(array(1,2,NULL,3,NULL,3));
# MAGIC
# MAGIC SELECT array_contains(array(1,2,3),9);
# MAGIC
# MAGIC SELECT array_insert(array(1,2,3), 2, 9);
# MAGIC
# MAGIC SELECT array_prepend(array(1,2,3), 0);
# MAGIC
# MAGIC SELECT array_position(array(1,3,3,4,2), 2);
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # UDFs In SparkSQL

# COMMAND ----------

def my_upper_function(input:str)->str:
    return input.upper()

# COMMAND ----------

spark.udf.register("my_upper", my_upper_function, StringType())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   my_upper('hello')

# COMMAND ----------

# MAGIC %md
# MAGIC # SparkSQL Using DF

# COMMAND ----------

display(spark.sql("SELECT * FROM {df_csv_var} WHERE price > 10 ", df_csv_var = df_csv))

# COMMAND ----------

# MAGIC %md
# MAGIC # SPARKSQL Query Files

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1) Using Views On Top Of Files

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_csv
# MAGIC USING CSV
# MAGIC OPTIONS(
# MAGIC   path "/Volumes/sparkcatalog/raw/source/raw_orders/orders.csv",
# MAGIC   header "true",
# MAGIC   inferSchema "true"
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM orders_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_json
# MAGIC USING org.apache.spark.sql.json
# MAGIC OPTIONS(
# MAGIC   path "/Volumes/sparkcatalog/raw/source/raw_orders/orders.json",
# MAGIC   header "true",
# MAGIC   inferSchema "true"
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM orders_json;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2) Using File Format Connector

# COMMAND ----------

display(spark.sql("SELECT * FROM json.`/Volumes/sparkcatalog/raw/source/raw_orders/orders.json`"))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Broadcast Hash Join

# COMMAND ----------

data1 = [(1,'abc',901),(2,'def',902),(3,'ghi',903),(4,'jkl',904),(5,'mno',905),(6,'pqr',906),(7,'stu',907),(8,'vwx',908),(9,'tuv',909),(10,'wxy',910),(11,'zab',911),(12,'bcd',912),(13,'efg',913),(14,'hij',914),(15,'klm',915),(16,'nop',916),(17,'qrs',917),(18,'tuv',918),(19,'wxy',919),(20,'zab',920),(21,'bcd',921),(22,'efg',922),(23,'hij',923),(24,'klm',924),(25,'nop',925)]

df1 = spark.createDataFrame(data1,['id','name','code'])

data2 = [(101,1),(102,2),(103,3),(104,4),(105,5),(106,6),(107,7),(108,8),(109,9),(110,10),(111,11),(112,12),(113,13),(114,14),(115,15),(116,31),(117,32),(118,33),(119,34),(120,35),(121,36),(122,37),(123,38),(124,39),(125,40)]
df2 = spark.createDataFrame(data2,['o_id','id'])

# Turn OFF the AQE
spark.conf.set("spark.sql.adaptive.enabled","false")

# Broadcast Join
df_broadcast = df1.join(broadcast(df2),df1['id']==df2['id'],'inner')
display(df_broadcast)



# COMMAND ----------

# MAGIC %md
# MAGIC # Cache & Persist

# COMMAND ----------

df_csv.cache()

# COMMAND ----------

display(df_csv)

# COMMAND ----------

from pyspark.storagelevel import StorageLevel

df_json.persist(StorageLevel.DISK_ONLY)


# COMMAND ----------

display(df_json)

# COMMAND ----------

df_csv.unpersist()
df_json.unpersist()


# COMMAND ----------

display(df_json)

# COMMAND ----------

# MAGIC %md
# MAGIC # Partitions in PySpark

# COMMAND ----------

df_part = spark.read.format("csv")\
      .option("header",True)\
      .option("inferSchema",True)\
      .load("/Volumes/sparkcatalog/raw/source/raw_orders/orders.csv")

display(df_part)

# COMMAND ----------

df_part = df_part.withColumn("year",year(col("order_date")))\
                .withColumn("month",month(col("order_date")))\
                .withColumn("day",dayofmonth(col("order_date")))

display(df_part)



# COMMAND ----------

df_part.write.format("parquet")\
            .mode("append")\
            .partitionBy("year","month","day")\
            .option("path","/Volumes/sparkcatalog/rawsource/orders_partitions/")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Partition Pruning

# COMMAND ----------

df_prune = spark.read.format("parquet")\
              .load("/Volumes/sparkcatalog/raw/source/orders_partitions/")\
              .filter((col("year") == 2025) & (col("month")==12) & (col("day")==25))

display(df_prune)

# COMMAND ----------

# MAGIC %md
# MAGIC # SALTING WITH GroupBY

# COMMAND ----------

data = [(1,'food'),(1,'food'),(1,'food'),(1,'food'),(1,'food'),(1,'food'),(1,'food'),(1,'food'),(1,'food'),(1,'food'),(1,'food'),(1,'food'),(1,'food'),(1,'food'),(1,'food'),(2,'electronics'),(2,'electronics'),(2,'electronics'),(2,'electronics'),(3,'clothes'),(3,'clothes'),(3,'clothes'),(3,'clothes'),(3,'clothes')]

df_skew = spark.createDataFrame(data,['id','category'])

display(df_skew)

# COMMAND ----------

df_salt = df_skew.withColumn("salt_col",floor(rand()*3))
display(df_salt)

# COMMAND ----------

df_salt = df_salt.withColumn("salt_group",concat(col('id'),lit('-'),col('salt_col')))
display(df_salt)

# COMMAND ----------

df_grp = df_salt.groupBy("salt_group").agg(count("category").alias("total_count"))
display(df_grp)

# COMMAND ----------

# MAGIC %md
# MAGIC # SALTING WITH JOINS

# COMMAND ----------

data = [(1,'edible'),(2,'lifestyle'),(3,'nature')]

df_small = spark.createDataFrame(data,["id","tag"])

# Adding Salat Array
df_small = df_small.withColumn("salt_array",array([lit(i) for i in range(3)]))

# Explode the salt_array
df_small = df_small.select("id","tag",explode("salt_array").alias("salt_col"))
display(df_small)

# COMMAND ----------

display(df_salt)

# COMMAND ----------

df_join = df_salt.join(df_small,(df_salt['id']==df_small['id'])&(df_salt['salt_col']==df_small['salt_col']),"left")
display(df_join)

# COMMAND ----------

# MAGIC %md
# MAGIC # SQL Hints

# COMMAND ----------

# Hypothetical Example
df1 = df_salt
df2 = df_small

df1.createOrReplaceTempView("df1")
df2.createOrReplaceTempView("df2")


# SQL Hints
display(spark.sql("""
SELECT /*+ BROADCAST(df2) */ 
* 
FROM 
    df1 
JOIN df2 
ON df1.id = df2.id AND df1.salt_col = df2.salt_col"""))

# COMMAND ----------

# MAGIC %md
# MAGIC # Broadcast Variable

# COMMAND ----------

my_map = {
    "1" : "edible",
    "2" : "lifestyle",
    "3" : "technology"
}

# COMMAND ----------

spark.sparkContext.broadcast(my_map)

# COMMAND ----------

map_value = spark.sparkContext.broadcast(my_map)
map_value.value

# COMMAND ----------

map_value.value['1']

# COMMAND ----------



# COMMAND ----------


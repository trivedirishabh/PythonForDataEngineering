# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC # Append Mode

# COMMAND ----------

# Enabling Schema Inference 
spark.conf.set("spark.sql.streaming.streaming.schemaInference" ,"True")

# COMMAND ----------

df_batch = spark.read.format("json")\
        .option("multiLine",True)\
        .option("inferSchema",True)\
        .load("/Volumes/sparkcatalog/raw/source/src_append/")

json_schema = df_batch.schema

# COMMAND ----------

df = spark.readStream.format("json")\
        .option("multiLine",True)\
        .schema(json_schema)\
        .option("cleanSource","archive")\
        .option("sourceArchiveDir","/Volumes/sparkcatalog/raw/source/source_archive/")\
        .load("/Volumes/sparkcatalog/raw/source/src_append/")


# COMMAND ----------

query = df.writeStream.format("delta")\
        .outputMode("append")\
        .trigger(processingTime="5 seconds")\
        .option("checkpointLocation", "/Volumes/sparkcatalog/raw/source/sink_append/checkpoint")\
        .option("path", "/Volumes/sparkcatalog/raw/source/sink_append/data")\
        .start()

# COMMAND ----------

query.stop()

# COMMAND ----------

df_res = spark.read.format("delta")\
            .load("/Volumes/sparkcatalog/raw/source/sink_append/data")

display(df_res)

# COMMAND ----------

# MAGIC %md
# MAGIC # Complete Mode

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sparkcatalog.raw.complete_source
# MAGIC (
# MAGIC   color STRING
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sparkcatalog.raw.complete_source
# MAGIC VALUES ('red')

# COMMAND ----------

df = spark.readStream.table('sparkcatalog.raw.complete_source')

df = df.groupBy('color').agg(count('color').alias('count'))

df.writeStream.format("delta")\
        .outputMode("complete")\
        .trigger(processingTime="5 seconds")\
        .option("checkpointLocation", "/Volumes/sparkcatalog/raw/source/sink_complete/checkpoint")\
        .option("path", "/Volumes/sparkcatalog/raw/source/sink_complete/data")\
        .start()


# COMMAND ----------

df_sink = spark.read.format("delta")\
            .load("/Volumes/sparkcatalog/raw/source/sink_complete/data")

display(df_sink)

# COMMAND ----------

# MAGIC %md
# MAGIC # Trigger Modes

# COMMAND ----------

# Processing Time
df.writeStream.format("delta")\
        .outputMode("complete")\
        .trigger(processingTime="5 seconds")\
        .option("checkpointLocation", "/Volumes/sparkcatalog/raw/source/sink_complete/checkpoint")\
        .option("path", "/Volumes/sparkcatalog/raw/source/sink_complete/data")\
        .start()
    
# Once 
df.writeStream.format("delta")\
        .outputMode("complete")\
        .trigger(once=True)\
        .option("checkpointLocation", "/Volumes/sparkcatalog/raw/source/sink_complete/checkpoint")\
        .option("path", "/Volumes/sparkcatalog/raw/source/sink_complete/data")\
        .start()

# Available Now
df.writeStream.format("delta")\
        .outputMode("complete")\
        .trigger(availableNow=True)\
        .option("checkpointLocation", "/Volumes/sparkcatalog/raw/source/sink_complete/checkpoint")\
        .option("path", "/Volumes/sparkcatalog/raw/source/sink_complete/data")\
        .start()

# Continuous (Not Supported Yet With Delta Tables)
df.writeStream.format("delta")\
        .outputMode("complete")\
        .trigger(continuous='1 second')\
        .option("checkpointLocation", "/Volumes/sparkcatalog/raw/source/sink_complete/checkpoint")\
        .option("path", "/Volumes/sparkcatalog/raw/source/sink_complete/data")\
        .start()

# COMMAND ----------

# MAGIC %md
# MAGIC # ForEachBatch

# COMMAND ----------

df = spark.readStream.table('sparkcatalog.raw.complete_source')



# COMMAND ----------

def my_function(df,batch_id):

    # Writing To Stream 1 (Will Have To Use Batch Code)
    df.write.format("parquet")\
            .mode("overwrite")\
            .option("path","/Volumes/sparkcatalog/raw/source/streams/stream1")\
            .save()

    # Writing To Stream 2 (Will Have To Use Batch Code)
    df.write.format("parquet")\
            .mode("overwrite")\
            .option("path","/Volumes/sparkcatalog/raw/source/streams/stream2")\
            .save()

    # Can Use Batch Logic To Apply UPSERT 


# COMMAND ----------

df.writeStream.foreachBatch(my_function)\
        .outputMode("append")\
        .option("path","/Volumes/sparkcatalog/raw/source/streams/checkpoint")\
        .option("path","/Volumes/sparkcatalog/raw/source/streams/data")\
        .start()
        
        

# COMMAND ----------

# MAGIC %md
# MAGIC # WINDOWS

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Source Table
# MAGIC
# MAGIC CREATE TABLE sparkcatalog.raw.stream_source
# MAGIC (
# MAGIC   color STRING,
# MAGIC   event_time TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_timestamp()

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sparkcatalog.raw.stream_source
# MAGIC VALUES
# MAGIC ('GREEN','2026-02-12T20:08:18.360+00:00')

# COMMAND ----------

df_str = spark.readStream.table('sparkcatalog.raw.stream_source')\
                .groupBy('color',window('event_time','10 minutes')).agg(count('color').alias("count"))

# COMMAND ----------

df_str.writeStream.format("delta")\
        .outputMode("complete")\
        .option("checkpointLocation", "/Volumes/sparkcatalog/raw/source/window_sink/checkpoint")\
        .option("path", "/Volumes/sparkcatalog/raw/source/window_sink/data")\
        .start()

# COMMAND ----------

df_res = spark.read.format("delta").load("/Volumes/sparkcatalog/raw/source/window_sink/data")
display(df_res)

# COMMAND ----------

# MAGIC %md
# MAGIC ### WATERMARKS

# COMMAND ----------

df_str = spark.readStream.table('sparkcatalog.raw.stream_source')\
                .withWatermark('event_time','10 minutes')\
                .groupBy('color',window('event_time','10 minutes')).agg(count('color').alias("count"))
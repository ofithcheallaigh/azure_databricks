# Databricks notebook source
# MAGIC %md ### Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md #### Step 1: Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

pit_stop_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("stop", StringType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("duration", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True) 
])

# COMMAND ----------

pit_stops_df = spark.read\
.schema(pit_stop_schema)\
.option("multiline",True)\
.json("/mnt/formula1dlsof/raw/pit_stops.json")

# COMMAND ----------

# MAGIC %md #### Step 2: Rename columns and add new column
# MAGIC 1. rename driverId and raceId
# MAGIC 2. Add `ingestion_date` with current timestamp

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("raceId", "race_id")\
                       .withColumnRenamed("driverId", "driver_id")\
                       .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md #### Write the output to the process container in pargqet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formula1dlsof/process/pit_stops")

# COMMAND ----------


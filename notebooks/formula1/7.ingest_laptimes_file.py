# Databricks notebook source
# MAGIC %md ### Ingest lap_times folder

# COMMAND ----------

# MAGIC %md #### Step 1: Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True) 
])

# COMMAND ----------

lap_times_df = spark.read\
.schema(lap_times_schema)\
.csv("/mnt/formula1dlsof/raw/lap_times")

# COMMAND ----------

# MAGIC %md #### Step 2: Rename columns and add new column
# MAGIC 1. rename driverId and raceId
# MAGIC 2. Add `ingestion_date` with current timestamp

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("raceId", "race_id")\
                       .withColumnRenamed("driverId", "driver_id")\
                       .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md #### Write the output to the process container in pargqet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formula1dlsof/process/lap_times")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlsof/process/lap_times"))

# COMMAND ----------


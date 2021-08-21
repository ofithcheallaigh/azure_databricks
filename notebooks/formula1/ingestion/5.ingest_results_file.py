# Databricks notebook source
# MAGIC %md ### Ingest results.json file

# COMMAND ----------

# MAGIC %md #### Step 1: Read the JSON file using the spark dataframe API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultsId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True),
])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json("/mnt/formula1dlsof/raw/results.json")

# COMMAND ----------

# MAGIC %md #### Step 2: Rename columns and add new column

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultsId", "results_id")\
                                    .withColumnRenamed("raceId", "race_id")\
                                    .withColumnRenamed("driverId", "driver_id")\
                                    .withColumnRenamed("constructorId", "constructor_id")\
                                    .withColumnRenamed("positionText", "position_text")\
                                    .withColumnRenamed("positionOrder", "position_order")\
                                    .withColumnRenamed("fastestLap", "fastest_lap")\
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
                                    .withColumnRenamed("fatestLapSpeed", "fastest_lap_speed")\
                                    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md #### Step 3: Drop unwanted column

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md #### Step 4: Write the output to the process container in parquet format

# COMMAND ----------

results_final_df.write.mode('overwrite').partitionBy("race_id").parquet("/mnt/formula1dlsof/process/results")

# COMMAND ----------

# display(spark.read.parquet("/mnt/formula1dlsof/process/results"))

# COMMAND ----------


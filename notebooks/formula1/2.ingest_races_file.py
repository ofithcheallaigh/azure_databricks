# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest races.csv file

# COMMAND ----------

# MAGIC %md #### Step 1: Read the CSV file using the spark dataframe API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)
                                 ])

# COMMAND ----------

  races_df = spark.read \
  .option("header", True) \
  .schema(races_schema) \
  .csv("/mnt/formula1dlsof/raw/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md #### Step 3: Select only the columns required & renaming as required

# COMMAND ----------

races_select_df = races_with_timestamp_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"),
                                                                                                   col("name"), col("ingestion_date"), col("race_timestamp"))

# COMMAND ----------

# MAGIC %md #### Step 4: Write the output to process container in parquet format

# COMMAND ----------

races_select_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/formula1dlsof/process/races')

# COMMAND ----------


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

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md #### Step 3: Select only the columns required

# COMMAND ----------

races_select_df = races_with_timestamp_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"),
                                                                                                   col("name"), col("ingestion_date"), col("race_timestamp"))

# COMMAND ----------


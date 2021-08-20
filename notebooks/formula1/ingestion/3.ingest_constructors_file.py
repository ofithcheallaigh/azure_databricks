# Databricks notebook source
# MAGIC %md ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md #### Step 1: Read the json file using the Spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
.schema(constructors_schema) \
.json("/mnt/formula1dlsof/raw/constructors.json")

# COMMAND ----------

# constructors_df.printSchema()

# COMMAND ----------

# display(constructors_df)

# COMMAND ----------

# MAGIC %md ### Step 2: Drop unwanted columns for the dataframe

# COMMAND ----------

constructor_dropped_df = constructors_df.drop("url")
# constructor_dropped_df = constructors_df.drop(constructors_df['url'])
# from pyspark.sql.functions import col
# constructor_dropped_df = constructors_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md #### Step 3: Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id")\
                                              .withColumnRenamed("constructorRef", "constructor_ref")\
                                              .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# display(constructor_final_df)

# COMMAND ----------

# MAGIC %md #### Step 4: Write the data to the parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/formula1dlsof/process/constructors")

# COMMAND ----------

# %fs
# ls /mnt/formula1dlsof/process/constructors

# COMMAND ----------


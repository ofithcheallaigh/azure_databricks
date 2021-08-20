# Databricks notebook source
# MAGIC %md ### Ingest drivers.json file

# COMMAND ----------

# MAGIC %md #### Step 1: Read the JSON file using the pyspark dataframe API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# MAGIC %md #### Step 2: Rename columns and add new columns
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation of forename and surname

# COMMAND ----------



# COMMAND ----------

# MAGIC %md #### Step 3: Drop the unwanted columns
# MAGIC 1. name.forename
# MAGIC 2. name.surname
# MAGIC 3. url

# COMMAND ----------



# COMMAND ----------

# MAGIC %md #### Write the output to processed container in parquet format

# COMMAND ----------


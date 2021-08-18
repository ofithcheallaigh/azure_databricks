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

display(constructors_df)

# COMMAND ----------

# MAGIC %md ### Step 2: Drop unwanted columns for the dataframe

# COMMAND ----------

constructor_dropped_df = constructors_df.drop("url")
# constructor_dropped_df = constructors_df.drop(constructors_df['url'])

# COMMAND ----------


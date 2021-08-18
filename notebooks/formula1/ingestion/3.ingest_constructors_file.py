# Databricks notebook source
# MAGIC %md ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md #### Read the json file using the Spark dataframe reader

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


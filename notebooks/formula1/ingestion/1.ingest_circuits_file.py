# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using the spark dataframe reader

# COMMAND ----------

circuits_df = spark.read.csv("dbfs:/mnt/formula1dlsof/raw/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC The two cells below are used to find the location of the data which can then be used on the cell above.

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlsof/raw

# COMMAND ----------

# MAGIC %md
# MAGIC Some basic instructions we can carry out are shown below:

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------


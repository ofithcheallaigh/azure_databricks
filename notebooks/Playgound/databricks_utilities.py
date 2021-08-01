# Databricks notebook source
# MAGIC %fs
# MAGIC ls

# COMMAND ----------

dbutils.fs.ls('/')
# This is the way to list the files using utils

# COMMAND ----------

for folder_name in dbutils.fs.ls('/'):
  print(folder_name)
# We can combine python code with the dbutilis 
# we can do this with R and SQL also

# COMMAND ----------

dbutils.fs.help()
# Help command

# COMMAND ----------

dbutils.fs.help("mount")
# Getting help on the mount method

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

dbutils.notebook.run("./child_notebook", 20)
# This calls out child notebook, timeout is 10 seconds

# COMMAND ----------


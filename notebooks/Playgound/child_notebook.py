# Databricks notebook source
dbutils.widgets.text("input", "", "Send the parameter value")

# COMMAND ----------

input_param = dbutils.widgets.get("input")

# COMMAND ----------

input_param

# COMMAND ----------

print("I am the child notebook")

# COMMAND ----------

dbutils.notebook.exit(100)
# Exit command from this NB.
# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using the spark dataframe reader

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.csv("dbfs:/mnt/formula1dlsof/raw/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Below, we are going to define our schema. This will be used on down insteat of the option to infer schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitID", IntegerType(), False),
                                     StructField("circuitRef", StringType(), False),
                                     StructField("name", StringType(), False),
                                     StructField("location", StringType(), False),
                                     StructField("country", StringType(), False),
                                     StructField("lat", DoubleType(), False),
                                     StructField("lng", DoubleType(), False),
                                     StructField("alt", IntegerType(), False),
                                     StructField("url", StringType(), False)
])

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

# MAGIC %md
# MAGIC The above show() command is useful, but truncates the data, we can use the display command also:

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Looking at the data we can see that it hasn't identified the correct header, so to do this, we can use the following command:

# COMMAND ----------

circuits_df = spark.read.option("header", True).csv("dbfs:/mnt/formula1dlsof/raw/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We can now see that we have the correct header in place for the DataFrame. Next we will print the schema

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC The schema doesn't look right -- everything is set as a string, when some should be ints or doubles. We can try to make the schema more, correct, with the following code, mist of which has been used above:

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC This schema is fine. However, the better way would be to decide on the schema and have the data apply to it. 
# MAGIC 
# MAGIC Omnce schema has been changed, it we can display the DF below

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Select only the required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitID", "circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuitID, circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lng,circuits_df.alt)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df["circuitID"],circuits_df["circuitRef"],circuits_df["name"],circuits_df["location"],
                                          circuits_df["country"],circuits_df["lat"],circuits_df["lng"],circuits_df["alt"])

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitID"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC Using any of the last three methods allows us to apply functions to the data we are selecting

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitID"),col("circuitRef"),col("name"),col("location"),col("country").alias("race_country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Renaming columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitID","circuit_id") \
.withColumnRenamed("circuitRef","circuit_ref") \
.withColumnRenamed("lat","latitude") \
.withColumnRenamed("lng","longitude") \
.withColumnRenamed("alt","altitude")

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Adding ingestion data to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Write data to datalake as parquet file

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{process_folder_path}/circuits")
# overwrite allows us to keep writing data, otherwise would get an error becuase file and path already exists.

# COMMAND ----------


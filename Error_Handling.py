# Databricks notebook source
from pyspark.sql.types import *
df_schema = StructType([StructField("name",StringType(),True), StructField("citizenShip",StringType(),True), StructField("CountryBirth",StringType(),True),
                       StructField("Age",IntegerType(),True),StructField("corrupt_record",StringType(),True)])

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/Error_Handling.csv",header=False,schema=df_schema)

# COMMAND ----------

df.show()

# COMMAND ----------

df_P = spark.read.option("mode","PERMISSIVE").option("columnNameOfCorruptRecord","corrupt_record").csv("dbfs:/FileStore/tables/Error_Handling.csv",header=False,schema=df_schema)

# COMMAND ----------

df_P.show()

# COMMAND ----------

df_f = spark.read.option("mode","FAILFAST").
option("columnNameOfCorruptRecord","corrupt_record").
csv("dbfs:/FileStore/tables/Error_Handling.csv",
    header=False,schema=df_schema)

# COMMAND ----------

try:
    df_f = (spark.read.option("mode","FAILFAST")
    .option("columnNameOfCorruptRecord","corrupt_record")
    .csv("dbfs:/FileStore/tables/Error_Handling.csv",header=False,schema=df_schema))
    
    df_f.show()
except Exception as e:
    print("load failed")

# COMMAND ----------

try:
    df_d = (spark.read.option("mode","DROPMALFORMED")
    .option("columnNameOfCorruptRecord","corrupt_record")
    .csv("dbfs:/FileStore/tables/Error_Handling.csv",header=False,schema=df_schema))
    
    df_d.show()
except Exception as e:
    print("load failed")

# COMMAND ----------

from pyspark.sql.types import *
df_schema_bad = StructType([StructField("name",StringType(),True), StructField("citizenShip",StringType(),True), StructField("CountryBirth",StringType(),True),
                       StructField("Age",IntegerType(),True)])

# COMMAND ----------

df_bad = (spark.read.option("badRecordsPath","/FileStore/tables/logs/")
    .csv("dbfs:/FileStore/tables/Error_Handling.csv",header=False,schema=df_schema_bad))

df_bad.show()


# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/logs/

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/logs/20220320T154815/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/logs/20220320T154815/bad_records/

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/tables/logs/20220320T154815/bad_records/part-00000-73143d89-b6c7-4cf6-9566-471ee8ad3634

# COMMAND ----------


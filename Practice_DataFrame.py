# Databricks notebook source
sqlContext

# COMMAND ----------

spark #for spark session

# COMMAND ----------

df = spark.createDataFrame([(1,"Siva"),(2,"Sowjanya"),(3,"Lucky")],["ID","Name"])

# COMMAND ----------

df1 = spark.createDataFrame([[1,"Siva"],[2,"Sowjanya"],[3,"Lucky"],["ID","Name"]) #should use tuple for data

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables

# COMMAND ----------

df_csv = spark.read.csv("dbfs:/FileStore/tables/USA_Housing.csv")

# COMMAND ----------

display(df_csv)

# COMMAND ----------

df_csv.rdd.getNumPartitions()

# COMMAND ----------

df_csv_head = spark.read.csv("dbfs:/FileStore/tables/USA_Housing.csv",header = True)

# COMMAND ----------

df_csv_head.show(2)

# COMMAND ----------

display(df_csv_head)

# COMMAND ----------

df_csv_head.collect()

# COMMAND ----------

df_csv_head.limit(3)

# COMMAND ----------

df_csv_head.columns

# COMMAND ----------

df_csv_head.dtypes

# COMMAND ----------

df_csv_head.printSchema()

# COMMAND ----------

df_csv_head.schema

# COMMAND ----------

df_csv_head.describe()

# COMMAND ----------


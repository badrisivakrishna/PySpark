# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

df_emp = spark.read.csv("dbfs:/FileStore/tables/emp.csv",header=True,inferSchema=True)

# COMMAND ----------

display(df_emp)

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

df_emp.write.format("parquet").mode("overwrite").save("/fileFormats/parquet")

# COMMAND ----------

# MAGIC %fs ls "fileFormats/parquet"

# COMMAND ----------

# MAGIC %fs head dbfs:/fileFormats/parquet/part-00000-tid-4480523121890728637-87f534b0-de6f-4846-81a9-6fd1cb582496-4-1-c000.snappy.parquet

# COMMAND ----------

df_avro = spark.read.csv("dbfs:/FileStore/tables/emp.csv",header=True,inferSchema=True)

# COMMAND ----------

df_avro.printSchema()

# COMMAND ----------

df_avro.write.format("avro").save("/fileFormats/avro")

# COMMAND ----------

# MAGIC %fs ls /fileFormats/avro

# COMMAND ----------

# MAGIC %fs head dbfs:/fileFormats/avro/part-00000-tid-3654080198043495029-ad93c5cb-da19-4fc5-8029-88b6ac51e1d5-7-1-c000.avro

# COMMAND ----------

df_avro.write.format("orc").save("/fileFormats/orc")

# COMMAND ----------

# MAGIC %fs ls /fileFormats/orc

# COMMAND ----------

# MAGIC %fs head dbfs:/fileFormats/orc/part-00000-tid-6721497877959251442-65877663-5274-4fca-83f3-39e13226d8de-8-1-c000.snappy.orc

# COMMAND ----------

spark.read.parquet("/fileFormats/parquet").show()

# COMMAND ----------

spark.read.parquet("dbfs:/fileFormats/parquet/part-00000-tid-4480523121890728637-87f534b0-de6f-4846-81a9-6fd1cb582496-4-1-c000.snappy.parquet").show()

# COMMAND ----------

# MAGIC %fs ls dbfs:/fileFormats/parquet

# COMMAND ----------

spark.read.format("avro").load("/fileFormats/avro").show()

# COMMAND ----------

spark.read.orc("/fileFormats/orc").show()

# COMMAND ----------


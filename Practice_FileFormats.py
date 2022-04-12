# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables

# COMMAND ----------

df_emp = spark.read.csv("dbfs:/FileStore/tables/emp.csv",header=True)

# COMMAND ----------

df_emp.show()

# COMMAND ----------

# MAGIC %fs ls /fileFormats/

# COMMAND ----------

df_emp.write.format("parquet").mode("overwrite").save("dbfs:/fileFormats/parquet_p/")

# COMMAND ----------

# MAGIC %fs ls /fileFormats/parquet_p

# COMMAND ----------

# MAGIC %fs head dbfs:/fileFormats/parquet_p/_committed_7328204511718804415

# COMMAND ----------

df_emp.write.format("parquet").mode("overwrite").saveAsTable("emp_parquet_File")

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/emp_parquet_file/

# COMMAND ----------

df_par = spark.read.parquet("dbfs:/user/hive/warehouse/emp_parquet_file")

# COMMAND ----------

df_par.show()

# COMMAND ----------

# MAGIC %fs head dbfs:/user/hive/warehouse/emp_parquet_file/_committed_7041370239862368138

# COMMAND ----------



# COMMAND ----------

display(spark.read.text("dbfs:/user/hive/warehouse/emp_parquet_file/part-00000-tid-7041370239862368138-0cd8649c-5cb3-4221-b0a3-4b6638aa5313-5-1-c000.snappy.parquet"))

# COMMAND ----------

df_emp.write.format("avro").save("dbfs:/fileFormats/avro_av/")

# COMMAND ----------

# MAGIC %fs ls "dbfs:/fileFormats/avro_av/"

# COMMAND ----------

# MAGIC %fs head dbfs:/fileFormats/avro_av/_committed_4843386061962883522

# COMMAND ----------

display(spark.read.text("dbfs:/fileFormats/avro_av/part-00000-tid-4843386061962883522-16d790b9-caeb-4692-96fd-7edcca026110-8-1-c000.avro"))


# COMMAND ----------

df_emp.write.format("avro").mode("overwrite").save("dbfs:/fileFormats/avro_av/")

# COMMAND ----------

# MAGIC %fs ls "dbfs:/fileFormats/avro_av/"

# COMMAND ----------

df_emp.write.format("avro").mode("overwrite").saveAsTable("emp_avro_format")

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/emp_avro_format

# COMMAND ----------

df_emp.write.format("orc").save("dbfs:/fileFormats/orc_file")

# COMMAND ----------

# MAGIC %fs ls dbfs:/fileFormats/orc_file

# COMMAND ----------

df_emp.write.format("orc").mode("overwrite").save("dbfs:/fileFormats/orc_file")

# COMMAND ----------

# MAGIC %fs ls dbfs:/fileFormats/orc_file

# COMMAND ----------

display(spark.read.text("dbfs:/fileFormats/orc_file/part-00000-tid-1946176541326202762-021a5a81-6136-4254-a9be-de1fee6436a7-14-1-c000.snappy.orc"))

# COMMAND ----------


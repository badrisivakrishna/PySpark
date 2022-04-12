# Databricks notebook source
df_csv = spark.read.format("csv").option("header","true").option("inferschema","true").load("dbfs:/FileStore/tables/emp.csv")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables

# COMMAND ----------

display(df_csv)

# COMMAND ----------

df_csv.write.format("delta").mode("append").saveAsTable("emp_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_delta

# COMMAND ----------

import pyspark.sql.functions as f
df_csv = df_csv.withColumn("total_sal",f.col("sal")+f.col("comm"))
display(df_csv)

# COMMAND ----------

df_csv.write.format("delta").mode("append").saveAsTable("emp_delta")

# COMMAND ----------

#if there is new colmns then we have to provide option("mergeSchema","true")
df_csv.write.format("delta").mode("append").option("mergeSchema","true").saveAsTable("emp_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_delta

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

df_adult = spark.read.csv("dbfs:/databricks-datasets/adult/",inferSchema=True)

# COMMAND ----------

display(df_adult)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table delta. /mnt/data/adult

# COMMAND ----------

df_adult.write.format("delta").mode("overwrite").partitionBy("_c3").save("/mnt/data/adult")

# COMMAND ----------

# MAGIC %fs ls /mnt/data/adult

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta. `/mnt/data/adult`

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/airlines/

# COMMAND ----------


# Databricks notebook source
list_data = [1,2,3,4,5,6,7,8]
rdd_data = sc.parallelize(list_data)

# COMMAND ----------

rdd_data.getNumPartitions()

# COMMAND ----------

rdd_data.glom().collect()

# COMMAND ----------

rdd_data.collect()

# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/COVID/

# COMMAND ----------

rdd_readme = sc.textFile("dbfs:/databricks-datasets/README.md")

# COMMAND ----------

rdd_readme.collect()

# COMMAND ----------

rdd_data.collect()

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/airlines

# COMMAND ----------

dbutils.fs.head("dbfs:/databricks-datasets/airlines/part-00001")

# COMMAND ----------

# MAGIC %fs

# COMMAND ----------

sc.getConf().getAll()

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

# MAGIC %md
# MAGIC https://docs.gcp.databricks.com/data/databricks-file-system.html

# COMMAND ----------

rdd_covid.collect()

# COMMAND ----------

rdd_tuple = sc.parallelize((12,10,5,10))

# COMMAND ----------

rdd_tuple.getNumPartitions()

# COMMAND ----------

rdd_tuple.glom().collect()

# COMMAND ----------

rdd_tuple.collect()

# COMMAND ----------

rdd_lst = sc.parallelize([10,20,10,15,20,18,19,50,20,60,30])

# COMMAND ----------

type(rdd_lst)

# COMMAND ----------

rdd_lst.glom().collect()

# COMMAND ----------

rdd_lst.collect()

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

# MAGIC %fs head dbfs:/databricks-datasets/README.md

# COMMAND ----------

rdd_lst.top(3)

# COMMAND ----------

print(rdd_lst.first())
print(rdd_lst.stats())
print(rdd_lst.take(5))
print(rdd_lst.min())
print(rdd_lst.max())
print(rdd_lst.top(4))
print(rdd_lst.mean())
print(rdd_lst.stdev())
print(rdd_lst.count())
print(rdd_lst.sum())

# COMMAND ----------


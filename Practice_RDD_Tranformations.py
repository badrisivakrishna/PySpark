# Databricks notebook source
rdd_lst = sc.parallelize([20,30,40,50,60,70,80,90])

rdd_map = rdd_lst.map(lambda a:a+10)
rdd_filter = rdd_lst.filter(lambda a: a > 50)
print(rdd_map.collect())
print(rdd_filter.collect())

# COMMAND ----------

rdd_reuse_test = sc.parallelize([20,30,40,50,60,70,80,90])
rdd_reuse_test = rdd_reuse_test.filter(lambda a: a > 50)
rdd_reuse_test.collect()

# COMMAND ----------

rdd_reuse_test.collect()

# COMMAND ----------

del rdd_reuse_test

# COMMAND ----------

rdd_lst = sc.parallelize([20,30,40,50,60,70,80,90])

rdd_map = rdd_lst.map(lambda a:a+10)
rdd_filter = rdd_lst.filter(lambda a: a > 50)
del rdd_lst
print(rdd_map.collect())
print(rdd_filter.collect())

# COMMAND ----------

rdd_p = sc.parallelize([1,2,3,4,5,6,7,8])
rdd_p.saveAsTextFile("/rdd_Practice/rdd_partitions")

# COMMAND ----------

# MAGIC %fs ls dbfs:/rdd_Practice/rdd_partitions/

# COMMAND ----------

# MAGIC %fs head dbfs:/rdd_Practice/rdd_partitions/part-00000/

# COMMAND ----------

rdd_req_partitions = sc.parallelize([10,20,30,40,50,60,70,80],2)


# COMMAND ----------

rdd_req_partitions.getNumPartitions()

# COMMAND ----------

rdd_req_partitions.collect()

# COMMAND ----------

rdd_req_partitions.glom().collect()

# COMMAND ----------

rdd_req_partitions.sum()

# COMMAND ----------

rdd_req_partitions.count()

# COMMAND ----------

rdd_req_partitions.take(3)

# COMMAND ----------

rdd_req_partitions.stats()

# COMMAND ----------

rdd_req_partitions.stdev()

# COMMAND ----------

rdd_req_partitions.collect()

# COMMAND ----------

rdd_req_p = rdd_req_partitions.distinct()

# COMMAND ----------

rdd_req_p.collect()

# COMMAND ----------

lst_ran = [range(100,1)]

# COMMAND ----------

type(lst_ran)

# COMMAND ----------

lst_rana1 = [*range(10,100,1)]

# COMMAND ----------

lst_rana1

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

  
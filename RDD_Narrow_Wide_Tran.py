# Databricks notebook source
rdd_norrow = sc.parallelize([1,2,3,4,5,6,7,8],4)
rdd_norrow.glom().collect()

# COMMAND ----------

rdd_norrow.saveAsTextFile("/RDD/Norrw_Wide_Tran/")

# COMMAND ----------

# MAGIC %fs ls "/RDD/Norrw_Wide_Tran/"

# COMMAND ----------

# MAGIC %fs head dbfs:/RDD/Norrw_Wide_Tran/part-00000

# COMMAND ----------

# MAGIC %fs head dbfs:/RDD/Norrw_Wide_Tran/part-00001

# COMMAND ----------

# MAGIC %fs head dbfs:/RDD/Norrw_Wide_Tran/part-00002

# COMMAND ----------

# MAGIC %fs head dbfs:/RDD/Norrw_Wide_Tran/part-00003

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/RDD/Norrw_Wide_Tran/

# COMMAND ----------

# MAGIC %fs ls dbfs:/RDD/Norrw_Wide_Tran/

# COMMAND ----------

# MAGIC %fs ls dbfs:/RDD/

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

rdd_norrow.repartition(1).collect()

# COMMAND ----------

rdd_repartition =rdd_norrow.repartition(1)
rdd_repartition.collect()

# COMMAND ----------

rdd_repartition.glom().collect()

# COMMAND ----------

rdd_norrow.coalesce(1).collect() #it will go with only one stage but repartition will go multiple stages

# COMMAND ----------

rdd_re_coal = sc.parallelize([1,2,3,4,5,6,7,8,9,10],6)

# COMMAND ----------

print(rdd_re_coal.coalesce(3).glom().collect())
print(rdd_re_coal.repartition(3).glom().collect())

# COMMAND ----------

print(rdd_re_coal.coalesce(8).glom().collect()) # we can not increase the partitions
print(rdd_re_coal.repartition(8).glom().collect())

# COMMAND ----------

rdd_dups = sc.parallelize([1,1,1,2,2,3,3,3,3,4,4,4,5,6,7,7,7,8,8,8,9,9,9,9,9,9,1,1,1,1,2,2,2,2,3,3,3,4,4,4,5,5,5,6,6,6],6)

# COMMAND ----------

rdd_dups.glom().collect()

# COMMAND ----------

rdd_uniq = rdd_dups.distinct()

# COMMAND ----------

rdd_uniq.collect()

# COMMAND ----------

rdd_uniq.glom().collect()

# COMMAND ----------

rdd_test_dist = sc.parallelize([1,2,3,4,5,6],6)
rdd_test_dist.glom().collect()

# COMMAND ----------

rdd_test_dist.distinct().glom().collect()

# COMMAND ----------

rdd1_u = sc.parallelize([1,2,3,4,5,6],3)
rdd2_u = sc.parallelize([5,6,7,8,9,10],3)

# COMMAND ----------

rdd1_u.union(rdd2_u).glom().collect()

# COMMAND ----------

rdd1_u.union(rdd2_u).collect()

# COMMAND ----------

rdd1_u.intersection(rdd2_u).glom().collect()

# COMMAND ----------

rdd1_u.subtract(rdd2_u).glom().collect()

# COMMAND ----------


# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

# MAGIC %fs head dbfs:/databricks-datasets/SPARK_README.md

# COMMAND ----------

rdd_spark_data = sc.textFile("dbfs:/databricks-datasets/SPARK_README.md")

# COMMAND ----------

rdd_spark_data.collect()

# COMMAND ----------

rdd_spark_data.glom().collect()

# COMMAND ----------

rdd_spark_data.getNumPartitions()

# COMMAND ----------

rdd_spark_data.map(lambda a : a.split(" ")).collect()

# COMMAND ----------

#We have nested collection so need to flatten the list, Use flatmap

rdd_word = rdd_spark_data.flatMap(lambda a: a.split(" ")).filter(lambda b: b != "")

# COMMAND ----------

rdd_word_kv = rdd_word.map(lambda a: (a,1))

# COMMAND ----------

rdd_word_kv.keys().collect()

# COMMAND ----------

rdd_word_kv.values().collect()

# COMMAND ----------

rdd_spark_data.keys().collect()

# COMMAND ----------

rdd_word_kv.sortByKey().collect()

# COMMAND ----------

rdd_word_kv.reduceByKey(lambda a,b : a+b).collect()

# COMMAND ----------

rdd_word_kv.groupByKey()

# COMMAND ----------

rdd_word_kv.collect()

# COMMAND ----------

rdd_group = sc.parallelize([("A",5),("A",2),("B",3),("B",2),("A",5),("C",2),("C",5),("A",2),("B",5),("C",2)])
rdd_group.collect()

# COMMAND ----------

rdd_group.groupByKey().collect()

# COMMAND ----------

for k,v in rdd_group.groupByKey().collect():
    print("Keys: ",k)
    print("Values :",v)

# COMMAND ----------

#List Comprehensions

[(k,list(v))for k,v in rdd_group.groupByKey().collect()]

# COMMAND ----------

[(k,tuple(v))for k,v in rdd_group.groupByKey().collect()]

# COMMAND ----------

#we can use above list comprehences or mapValues function and this need a collection class like list, tuple
rdd_group.groupByKey().mapValues(list).collect()

# COMMAND ----------

rdd_group.groupByKey().mapValues(tuple).collect()

# COMMAND ----------

rdd_group.groupByKey().mapValues(set).collect()

# COMMAND ----------

rdd_group.groupByKey().mapValues(dict).collect()
#dict need key value pair so mapValues fun through an error

# COMMAND ----------

from operator import add
rdd_group.reduceByKey(add).collect()

# COMMAND ----------

rdd_group.groupByKey().collect()

# COMMAND ----------

rdd_group.groupByKey().mapValues(len).collect()

# COMMAND ----------

def my_add(x,y):
    x.add(y)
    return x
rdd_group.aggregateByKey(set(),my_add,lambda x,y: x.union(y)).collect()

# COMMAND ----------

rdd_group.collect()

# COMMAND ----------

def to_append(x,y):
    x.append(y)
    return x
def to_extend(x,y):
    x.extend(y)
    return x
rdd_group.aggregateByKey(list(),to_append,to_extend).collect()

# COMMAND ----------

rdd_group.groupByKey().mapValues(list).collect()

# COMMAND ----------

rdd_group.reduceByKey(len).collect()

# COMMAND ----------


# Databricks notebook source
import urllib.request
urllib.request.urlretrieve("https://www.gutenberg.org/files/1184/1184-0.txt","/tmp/theCount.txt")
dbutils.fs.mv("file:/tmp/theCount.txt","dbfs:/tmp/theCount.txt")

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/tmp/

# COMMAND ----------

# MAGIC %fs ls files:/temp

# COMMAND ----------

import urllib.request
urllib.request.urlretrieve("https://www.gutenberg.org/files/1184/1184-0.txt","/tmp/theCount.txt")

# COMMAND ----------

# MAGIC %fs ls file:/tmp/

# COMMAND ----------

# MAGIC %fs ls file:/

# COMMAND ----------

# MAGIC %fs head dbfs:/tmp/theCount.txt

# COMMAND ----------

rdd_count = sc.textFile("dbfs:/tmp/theCount.txt")

# COMMAND ----------

rdd_count.collect()

# COMMAND ----------

sample_str = "This is string splitting and stripping"
sample_str.lower().strip().split(" ")

# COMMAND ----------

rdd_count.flatMap(lambda a: a.lower().strip().split(" ")).collect()

# COMMAND ----------

from pyspark.ml.feature import StopWordsRemover
remover = StopWordsRemover()
stopWords = remover.getStopWords()
stopWords

# COMMAND ----------

rdd_count.flatMap(lambda a: a.lower().strip().split(" ")).filter(lambda a: a!="").filter(lambda b: b not in stopWords).collect()

# COMMAND ----------


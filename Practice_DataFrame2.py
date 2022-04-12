# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables

# COMMAND ----------

df_schema = spark.read.csv("dbfs:/FileStore/tables/emp.csv",header=True)

# COMMAND ----------

df_schema.show()

# COMMAND ----------

df_schema.schema

# COMMAND ----------

from pyspark.sql.types import *

v_schema = StructType([StructField("EmpNo",IntergerType(),True),StructField("ENAME",StringType(),True),StructField("JOB",StringType(),True), StructField("MGR",StringType(),True),StructField("HIREDATE",DateTimeType(),True),StructField("SAL",FloatType(),True),
                       StructField("SAL",FloatType(),True),StructField("COMM",FloatType(),True),StructField("DEPTNO",IntergerType(),True)])

# COMMAND ----------

import pyspark.sql.types as t

# COMMAND ----------

help(t)

# COMMAND ----------

df_schema.columns

# COMMAND ----------

df_schema.dtypes

# COMMAND ----------

df_schema.collect()

# COMMAND ----------

df_schema.show()

# COMMAND ----------

display(df_schema)

# COMMAND ----------

df_schema.describe().show()

# COMMAND ----------

df_remove_dups = df_schema.distinct()

# COMMAND ----------

df_remove_dups.show()

# COMMAND ----------

df_remove_dups.count()

# COMMAND ----------

df_schema.dropDuplicates().show()

# COMMAND ----------

df_schema.dropDuplicates(["COMM"]).show() # For specific columns

# COMMAND ----------

df_schema.dropna(how="any").show()

# COMMAND ----------

df_schema.dropna(how="all").show()

# COMMAND ----------

df_m = df_schema.dropna(how="all").replace("null",None)

# COMMAND ----------

df_m.fillna("N/A").show()

# COMMAND ----------

df_m.select("ENAME").show()

# COMMAND ----------

df_m.filter("JOB='CLERK'").show()

# COMMAND ----------

df_m.filter(df_m["JOB"]=="CLERK").show()

# COMMAND ----------

df_m.where("job='CLERK'").show() #col is not case sensitive but data is case sensitive

# COMMAND ----------

df_m.select("EMPNO","ENAME").show()

# COMMAND ----------


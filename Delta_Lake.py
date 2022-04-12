# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

df_csv = spark.read.option("nullValue","null").csv("dbfs:/FileStore/tables/emp.csv",header=True,inferSchema=True)

# COMMAND ----------

df_csv.show()

# COMMAND ----------

df_csv.write.format("parquet").mode("append").saveAsTable("emp_part")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_part

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_part where comm is null

# COMMAND ----------

# MAGIC %sql
# MAGIC update emp_part set comm = 0 WHERE COMM is null

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from emp_part where comm is null

# COMMAND ----------

df_csv.write.mode("append").saveAsTable("emp") #if we do not provide file format then it will create delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/emp

# COMMAND ----------

#If we save as table without any fileformat then it will save parquet file and delta support we have extra folder _delta_log
%fs ls dbfs:/user/hive/warehouse/emp/_delta_log

# COMMAND ----------

#If we create parquet file then this is the information will available
%fs ls dbfs:/user/hive/warehouse/emp_part

# COMMAND ----------

# MAGIC %sql
# MAGIC update emp set comm = 0 where comm is null
# MAGIC --Update will supprt in delta

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from emp where deptno=10
# MAGIC -- delete also support

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history emp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp version as of 2

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/emp/_delta_log/

# COMMAND ----------

# MAGIC %fs head dbfs:/user/hive/warehouse/emp/_delta_log/00000000000000000000.json

# COMMAND ----------

# MAGIC %sql show create table emp --if u see in the result it using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table emp_part
# MAGIC --here using parquet

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE TABLE `spark_catalog`.`default`.`emp` (
# MAGIC   `EMPNO` INT,
# MAGIC   `ENAME` STRING,
# MAGIC   `JOB` STRING,
# MAGIC   `MGR` INT,
# MAGIC   `HIREDATE` STRING,
# MAGIC   `SAL` INT,
# MAGIC   `COMM` INT,
# MAGIC   `DEPTNO` INT
# MAGIC ) USING delta

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/emp_part

# COMMAND ----------



# COMMAND ----------


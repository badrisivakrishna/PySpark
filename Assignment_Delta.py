# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

df_products = spark.read.text("dbfs:/FileStore/tables/Product.txt")

# COMMAND ----------

display(df_products.limit(10))

# COMMAND ----------

df_products.write.format("delta").mode("append").saveAsTable("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from products limit 10

# COMMAND ----------


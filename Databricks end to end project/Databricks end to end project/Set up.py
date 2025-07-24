# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME workspace.raw.rawvolume

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA workspace.gold

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/airports")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/workspace/bronze/bronzevolume/flights/data/`

# COMMAND ----------



# COMMAND ----------


# Databricks notebook source
# MAGIC %md
# MAGIC **A company receives daily sales transaction files from multiple store locations in Azure data lake folder. Instead of reprocessing all historical records every day, use spark structured streaming to increamentally load newly arrived files in delta table ensuring timely updates to analytical dashboard**

# COMMAND ----------

# MAGIC %md
# MAGIC ###**Streaming query**

# COMMAND ----------

my_schema = """
order_id INT,
customer_id INT,
order_date DATE,
amount DOUBLE
"""

# COMMAND ----------

# df_batch = spark.read.format("csv")\
#   .option("header", "true")\
#     .schema(my_schema)\
#       .load("/Volumes/pyspark_cata/source/db_volume/streamSource/")
# display(df_batch)

# COMMAND ----------

df = spark.readStream.format("csv")\
  .option("header", "true")\
    .schema(my_schema)\
      .load("/Volumes/pyspark_cata/source/db_volume/streamSource/")


# COMMAND ----------

# MAGIC %md
# MAGIC ## **Streaming output**

# COMMAND ----------

df.writeStream.format("delta")\
  .option("checkpointLocation", "/Volumes/pyspark_cata/source/db_volume/streamSink/checkpoint")\
    .option("mergeSchema", True)\
      .trigger(once = True)\
      .start("/Volumes/pyspark_cata/source/db_volume/streamSink/data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/pyspark_cata/source/db_volume/streamSink/data`
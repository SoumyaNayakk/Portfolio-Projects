# Databricks notebook source
# MAGIC %md
# MAGIC **A company recives daily updates for its product catalog, including new products, price changes, and discontinued items. Instead of overwriting the entire catalog or simply appending new records, they upsert the incoming data(update products with latest info and insert new products)**

# COMMAND ----------

# MAGIC %md
# MAGIC **Querying source**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pyspark_cata.source.products

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.sql("select * from pyspark_cata.source.products")
#deduplicate
df = df.withColumn("dedup", row_number().over(Window.partitionBy("id").orderBy(desc("updatedDate"))))
df = df.filter(col('dedup') == 1).drop("dedup")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##**Upsert**

# COMMAND ----------

#Creating delta object
from delta.tables import DeltaTable

if len(dbutils.fs.ls('/Volumes/pyspark_cata/source/db_volume/products_sink/')) > 0:
    dlt_obj = DeltaTable.forPath(spark, '/Volumes/pyspark_cata/source/db_volume/products_sink/')
    dlt_obj.alias("trg").merge(
        df.alias("src"),
        "src.id = trg.id")\
            .whenMatchedUpdateAll(condition="src.updatedDate >= trg.updatedDate")\
            .whenNotMatchedInsertAll()\
            .execute()
    print("This is upserting now")

else:
    df.write.format("delta")\
        .mode("Overwrite")\
        .save('/Volumes/pyspark_cata/source/db_volume/products_sink/')
    


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/pyspark_cata/source/db_volume/products_sink/`

# COMMAND ----------



# COMMAND ----------


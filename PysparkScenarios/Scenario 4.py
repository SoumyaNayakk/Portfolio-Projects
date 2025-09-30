# Databricks notebook source
# MAGIC %md
# MAGIC **Many pyspark notebooks require same set of customer transformation functions like data formating, null handling and data validation. Create python class to store these reusable functions.**

# COMMAND ----------

# MAGIC %md
# MAGIC ##**Python class**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

class DataValidation:
    
    def __init__(self,df):
        self.df = df
    def dedup(self,keyCol,cdcCol):
        df = self.df.withColumn("dedup",row_number().over(Window.partitionBy(keyCol).orderBy(desc(cdcCol))))
        df =df.filter(col('dedup')==1).drop('dedup')
        return df
    
    def removeNulls(self,nullCol):
        df = self.df.filter(col(nullCol).isNotNull())
        return df
    def upsert(self,target_path ):
        if len(dbutils.fs.ls(target_path)) > 0:
            dlt_obj = DeltaTable.forPath(target_path)
            dlt_obj.alias("trg").merge(
                self.df.alias("src"),
                "src.id = trg.id")\
                    .whenMatchedUpdateAll(condition="src.updatedDate >= trg.updatedDate")\
                    .whenNotMatchedInsertAll()\
                    .execute()
            print("This is upserting now")

        else:
            self.df.write.format("delta")\
                .mode("Overwrite")\
                .save(target_path)



# COMMAND ----------

df = spark.createDataFrame([("1","2020-01-01",100),("1","2020-01-02",200),("2","2020-01-02",200),("3","2020-01-03",300)],["order_id","order_timestamp","amount"])
display(df)

# COMMAND ----------

cls_obj = DataValidation(df)

# COMMAND ----------

df_dedup = cls_obj.dedup('order_id','order_timestamp')

# COMMAND ----------

display(df_dedup)

# COMMAND ----------

df = spark.createDataFrame([("1","2020-01-01",100),(None,"2020-01-02",200),("2","2020-01-02",200),("3","2020-01-03",300)],["order_id","order_timestamp","amount"])
display(df)


# COMMAND ----------

df_removeNulls = cls_obj.removeNulls('order_id')
display(df_removeNulls)

# COMMAND ----------

df = spark.createDataFrame([("1","2020-01-01",100),(None,"2020-01-02",200),("2","2020-01-02",200),("3","2020-01-03",300)],["order_id","order_timestamp","amount"])
display(df)

# COMMAND ----------

df_removeNulls = cls_obj.removeNulls('amount')
display(df_removeNulls)

# COMMAND ----------

df = spark.createDataFrame([("1","2020-01-01",100),("1","2020-01-02",500),("2","2020-01-02",200),("3","2020-01-03",300)],["order_id","order_timestamp","amount"])
display(df)

# COMMAND ----------

cls_obj = DataValidation(df)

# COMMAND ----------

df_upsert = cls_obj.upsert("/Volumes/pyspark_cata/source/db_volume/trialsink/")
display(df_upsert)


# COMMAND ----------

display(df_upsert)
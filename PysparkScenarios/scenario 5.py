# Databricks notebook source
# MAGIC %md
# MAGIC **A company maintains a product catalog in its data warehouse. Product details like name, category, price may change over time. To preserve historical data for reporting, we need to implement scd type 2**

# COMMAND ----------

# MAGIC %md
# MAGIC ##**SCD Type 2**

# COMMAND ----------

# MAGIC %sql
# MAGIC create table pyspark_cata.source.customers(
# MAGIC   id string,
# MAGIC   email string,
# MAGIC   city string,
# MAGIC   country string,
# MAGIC   modifiedDate timestamp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into pyspark_cata.source.customers values
# MAGIC ('1','abc@gmail.com','New York','USA',current_timestamp()),
# MAGIC ('2','john@gmail.com','London','UK',current_timestamp()),
# MAGIC ('3','mary@gmail.com','Paris','France',current_timestamp()),
# MAGIC ('4','peter@gmail.com','Tokyo','Japan',current_timestamp()),
# MAGIC ('5', 'xyz@gmail.com', 'Sydney','Australia',current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pyspark_cata.source.customers;

# COMMAND ----------

if spark.catalog.tableExists("pyspark_cata.source.DimCustomers"):
    pass
else:
    spark.sql("""
              create table pyspark_cata.source.DimCustomers
              select *, current_timestamp() as starttime,
              cast('3000-01-01' as timestamp) as endTime,
              'Y' as isActive

              from pyspark_cata.source.customers
              """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pyspark_cata.source.dimcustomers;

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

df = spark.sql("""
               select * from pyspark_cata.source.customers
               """)
df = df.withColumn("dedup",row_number().over(Window.partitionBy("id").orderBy(desc('modifiedDate'))))\
    .drop('dedup')
df= df.filter(col("dedup")==1)


display(df)
df.createOrReplaceTempView('src')

# COMMAND ----------

df.createOrReplaceTempView('srctemp')
df = spark.sql("""
              select *, current_timestamp() as starttime,
              cast('3000-01-01' as timestamp) as endTime,
              'Y' as isActive

              from srctemp
              """)

df.createOrReplaceTempView('src')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from src;

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## **Merge 1 Marking the updated records as expired**

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into pyspark_cata.source.DimCustomers as trg
# MAGIC using src as src
# MAGIC on trg.id = src.id
# MAGIC and trg.isActive = 'Y'
# MAGIC
# MAGIC when matched and src.email<>trg.email 
# MAGIC or src.city<>trg.city
# MAGIC or src.country<>trg.country
# MAGIC or src.modifiedDate<>trg.modifiedDate
# MAGIC
# MAGIC
# MAGIC then update set 
# MAGIC trg.endTime = current_timestamp(),
# MAGIC trg.isActive = 'N'
# MAGIC

# COMMAND ----------

df.createOrReplaceTempView('srctemp')
df = spark.sql("""
              select *, current_timestamp() as starttime,
              cast('3000-01-01' as timestamp) as endTime,
              'Y' as isActive

              from srctemp
              """)

df.createOrReplaceTempView('src')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Merge 2 inserting new + updated records**

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into pyspark_cata.source.DimCustomers as trg
# MAGIC using src as src
# MAGIC on src.id = trg.id
# MAGIC and trg.isActive = 'Y'
# MAGIC
# MAGIC when not matched then insert *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pyspark_cata.source.DimCustomers;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into pyspark_cata.source.customers values
# MAGIC ('1','abc@gmail.com','Seattle','USA',current_timestamp()),
# MAGIC ('6','john@gmail.com','London','UK',current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pyspark_cata.source.customers;
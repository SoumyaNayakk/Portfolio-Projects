# Databricks notebook source
# MAGIC %md
# MAGIC **An ecommerce platform service receives customer order details from mobile application in json format. It contains nested fields like customer info, payment details and list of purchased items. This should  be flattened using pyspark**

# COMMAND ----------

df = spark.read.format("json")\
    .option("inferSchema", True)\
        .option("multiLine", True)\
    .load("/Volumes/pyspark_cata/source/db_volume/jsonData/")
display(df)

# COMMAND ----------

df.schema

# COMMAND ----------

df_cust = df.select("customer.customer_id", "customer.email",'customer.location.city','customer.location.country',"*").drop("customer")
display(df_cust)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_cust_upd = df_cust.withColumn("delivery_updates", explode("delivery_updates"))\
    .withColumn("items", explode("items"))\
        .select("*")
display(df_cust_upd)

# COMMAND ----------

df_cust_fla= df_cust_upd.select("*","items.item_id","items.price_per_unit","items.product_name","items.quantity").drop("items")
display(df_cust_fla)

# COMMAND ----------

df_cust_final = df_cust_fla.select("*", "payment.amount","payment.currency","payment.method").drop("payment")
display(df_cust_final)

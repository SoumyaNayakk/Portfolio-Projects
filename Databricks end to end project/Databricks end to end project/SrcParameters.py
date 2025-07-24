# Databricks notebook source
#src_array = ["bookings", "airports", "customers", "flights"]

# COMMAND ----------

src_array =[
           {"src" : "bookings"},
           {"src" : "airports"},
           {"src" : "customers"},
           {"src" : "flights"}    
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "output_key", value = src_array)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


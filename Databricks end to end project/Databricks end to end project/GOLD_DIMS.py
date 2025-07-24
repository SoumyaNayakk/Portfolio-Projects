# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.silver_flights

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Parameters**

# COMMAND ----------

# #Catalog name
# catalog = "workspace"

# #key columns
# key_cols = "['flight_id']"
# key_cols_list = eval(key_cols)

# #cdc column
# cdc_col = "modifiedDate"

# #backdated refresh
# backdated_refresh = ""

# #source object
# source_object = "silver_flights"

# #source schema
# source_schema = "silver"

# #target schema
# target_schema = "gold"

# #target object
# target_object = "DimFlights"

# #surrogate key
# surrogate_key = "DimFlightsKey"

# COMMAND ----------

# #Catalog name
# catalog = "workspace"

# #key columns
# key_cols = "['airport_id']"
# key_cols_list = eval(key_cols)

# #cdc column
# cdc_col = "modifiedDate"

# #backdated refresh
# backdated_refresh = ""

# #source object
# source_object = "silver_airports"

# #source schema
# source_schema = "silver"

# #target schema
# target_schema = "gold"

# #target object
# target_object = "DimAirports"

# #surrogate key
# surrogate_key = "DimAirportsKey"

# COMMAND ----------

#Catalog name
catalog = "workspace"

#key columns
key_cols = "['passenger_id']"
key_cols_list = eval(key_cols)

#cdc column
cdc_col = "modifiedDate"

#backdated refresh
backdated_refresh = ""

#source object
source_object = "silver_passengers"

#source schema
source_schema = "silver"

#target schema
target_schema = "gold"

#target object
target_object = "DimPassengers"

#surrogate key
surrogate_key = "DimPassengersKey"

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Incremental data ingestion**

# COMMAND ----------

# MAGIC %md
# MAGIC **Last load date**

# COMMAND ----------

#No backdated refresh
if len(backdated_refresh) == 0:
# If table exists in the destination
  if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    last_load = spark.sql(f"Select max({cdc_col}) from workspace.{target_schema}.{target_object}").collect()[0][0]
    
  else:
    last_load = "1900-01-01 00:00:00"
#yes back dated refresh
else:
  last_load = backdated_refresh

#test the last load
    
  

# COMMAND ----------

last_load

# COMMAND ----------

df_src = spark.sql(f"Select * from {source_schema}.{source_object} where {cdc_col} > '{last_load}'")

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Old vs New records

# COMMAND ----------

#key columns string
key_cols_string =', '.join(key_cols_list)

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
  #key columns string for incremental
  key_cols_string_incremental =", ".join(key_cols_list)
  df_trg = spark.sql(f"SELECT {key_cols_string_incremental}, {surrogate_key}, create_date,update_date from {catalog}.{target_schema}.{target_object}")
else:
  #key columns string for initial
  key_cols_string_init = [f"'' as {i}" for i in key_cols_list ]
  key_cols_string_init = ", ".join(key_cols_string_init)
  
  df_trg = spark.sql(f"""SELECT {key_cols_string_init}, cast('0' as int) as {surrogate_key}, cast('1900-01-01 00:00:00' as TIMESTAMP) as create_date,cast('1900-01-01 00:00:00' as TIMESTAMP) as update_date where 1=0""" )

  

# COMMAND ----------

df_trg.display()

# COMMAND ----------

key_cols_string_incremental =', '.join(key_cols_list)

# COMMAND ----------

spark.sql(f"SELECT {key_cols_string} from {catalog}.{source_schema}.{source_object}")

# COMMAND ----------

key_cols_list

# COMMAND ----------



# COMMAND ----------

spark.sql(f"SELECT '' as flight_id, '' as DimFlightsKey, '1900-01-01 00:00:00' as create_date, '1900-01-01 00:00:00' as update_date from workspace.silver.silver_flights" ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Join condition**

# COMMAND ----------

join_condition = ' AND '.join([f"src.{i} = trg.{i}"for i in key_cols_list])

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("trg")

df_join = spark.sql(f"""
          select src.*,
                 trg.{surrogate_key},
                 trg.create_date,
                trg.update_date
          from src
          left join trg
          on {join_condition}
          """)



# COMMAND ----------

df_join.display()

# COMMAND ----------

#old records
df_old = df_join.filter(col(f'{surrogate_key}').isNotNull())
#new records
df_new = df_join.filter(col(f'{surrogate_key}').isNull())




# COMMAND ----------

df_old.display()
#df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##**Preparing df_old**

# COMMAND ----------

df_old_enr = df_old.withColumn('update_date',current_timestamp())


# COMMAND ----------

df_old_enr.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##**Preparing df_new**

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
  max_surrogate_key = spark.sql(f"""
                            select max({surrogate_key})  from {catalog}.{target_schema}.{target_object}
                        """).collect()[0][0]
  df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                 .withColumn('create_date',current_timestamp())\
                 .withColumn('update_date',current_timestamp())

else:
  max_surrogate_key = 0
  df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                 .withColumn('create_date',current_timestamp())\
                 .withColumn('update_date',current_timestamp())

   
  

# COMMAND ----------

df_new_enr.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ### **Unioning old and new records**

# COMMAND ----------

df_union = df_old_enr.unionByName(df_new_enr)


# COMMAND ----------

df_union.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###**Upsert**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
  dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
  dlt_obj.alias("trg").merge(df_union.alias("src"), f"trg.{surrogate_key} = src.{surrogate_key}")\
       .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= trg.{cdc_col}")\
       .whenNotMatchedInsertAll() \
       .execute()
else:
  df_union.write.format("delta")\
           .mode("append")\
           .saveAsTable(f"{catalog}.{target_schema}.{target_object}")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.DimPassengers
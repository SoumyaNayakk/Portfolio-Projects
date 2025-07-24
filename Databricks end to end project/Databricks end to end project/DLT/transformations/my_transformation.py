
import dlt  
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Bronze stage table
@dlt.table(
    name="stage_brookings"
)
def stage_brookings():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data")
    return df

# Transformation view
@dlt.view(
    name="trans_bookings"
)
def trans_bookings():
    df = spark.readStream.table("stage_brookings")
    df = df.withColumn("amount", col("amount").cast(DoubleType()))\
        .withColumn("modifiedDate", current_timestamp())\
        .withColumn("booking_date", to_date(col("booking_date")))\
        .drop("_rescued_data")
    return df

# Validation rules
rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL",
}

# Silver table with expectations
@dlt.table(
    name="silver_bookings"
)
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    df = spark.readStream.table("trans_bookings")
    return df
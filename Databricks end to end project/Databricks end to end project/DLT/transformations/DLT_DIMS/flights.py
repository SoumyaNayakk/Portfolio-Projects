import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define a DLT view to transform the streaming source
@dlt.view(
    name="trans_flights"
)
def trans_flights():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/flights/data")
    return df

# Create a streaming table for the silver layer
dlt.create_streaming_table("silver_flights")

# Set up Change Data Capture (CDC) flow with SCD Type 1
dlt.create_auto_cdc_flow(
    target="silver_flights",
    source="trans_flights",
    keys=["flight_id"],
    stored_as_scd_type=1
)
from pyspark import pipelines as dp
from pyspark.sql import *

@dp.table
@dp.expect("valid_customer_id", "customer_id is NOT NULL")
def customers_staging():
    df = spark.readStream.table("training2.source.customers")
    return df


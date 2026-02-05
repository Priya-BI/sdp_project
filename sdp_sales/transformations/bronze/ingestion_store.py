from pyspark import pipelines as dp
from pyspark.sql import *

@dp.table
@dp.expect("valid_store_sk", "store_sk is NOT NULL")
def store_staging():
    df = spark.readStream.table("training2.source.store")
    return df
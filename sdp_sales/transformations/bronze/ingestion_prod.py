from pyspark import pipelines as dp
from pyspark.sql import *

@dp.table
@dp.expect("valid_product_id", "product_id is NOT NULL")

def products_staging():
    df = spark.readStream.table("training2.source.products")
    return df
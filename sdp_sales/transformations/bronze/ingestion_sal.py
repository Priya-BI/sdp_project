from pyspark import pipelines as dp
from pyspark.sql import *

rules = {"rule1": "sales_id is NOT NULL",
         "rule2": "quantity > 0",
         "rule3": "amount > 0",
         "rule4": "product_id is NOT NULL"}

dp.create_streaming_table("sales_staging",expect_all=rules)

@dp.append_flow(target = "sales_staging")
def sales_north():
    df = spark.readStream.table("training2.source.sales_north")
    return df

@dp.append_flow(target = "sales_staging")
def sales_south():
    df = spark.readStream.table("training2.source.sales_south")
    return df
from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.temporary_view
def prod_enr():
    df = spark.readStream.table("products_staging")
    df = df.withColumn("price",col("price").cast("double"))
  
    return df

dp.create_streaming_table("products_enriched")

dp.create_auto_cdc_flow(
  target = "products_enriched",
  source = "prod_enr",
  keys = ["product_id"],
  sequence_by = col("last_updated"),
  #apply_as_deletes = expr("operation = 'DELETE'"),
  #except_column_list = ["operation", "sequenceNum"],
  stored_as_scd_type = "2"
)

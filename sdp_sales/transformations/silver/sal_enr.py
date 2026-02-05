from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.temporary_view

def sales_enr():
    df = spark.readStream.table("sales_staging")
    df = df.withColumn("total_amount",col("quantity") * col("amount"))
    return df

dp.create_streaming_table("sales_enriched")
dp.create_auto_cdc_flow(
  target = "sales_enriched",
  source = "sales_enr",
  keys = ["sales_id"],
  sequence_by = "sale_timestamp",
  #apply_as_deletes = expr("operation = 'DELETE'"),
  #except_column_list = ["operation", "sequenceNum"],
  stored_as_scd_type = "1"
)

from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.temporary_view
def store_enr():
    df = spark.readStream.table("store_staging")
    df = df.withColumn("country",upper(col("country")))
    df =  df.withColumnRenamed("store_sk", "store_id")
  
    return df

dp.create_streaming_table("store_enriched")

dp.create_auto_cdc_flow(
  target = "store_enriched",
  source = "store_enr",
  keys = ["store_id"],
  sequence_by = col("store_id"),
  #apply_as_deletes = expr("operation = 'DELETE'"),
  #except_column_list = ["operation", "sequenceNum"],
  stored_as_scd_type = "2"
)

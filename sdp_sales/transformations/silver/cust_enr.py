from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.temporary_view
def cust_enr():
    df = spark.readStream.table("customers_staging")
    df = df.withColumn("customer_name",upper(col("customer_name")))

    return df

dp.create_streaming_table("customers_enriched")

dp.create_auto_cdc_flow(
  target = "customers_enriched",
  source = "cust_enr",
  keys = ["customer_id"],
  sequence_by = col("last_updated"),
  #apply_as_deletes = expr("operation = 'DELETE'"),
  #except_column_list = ["operation", "sequenceNum"],
  stored_as_scd_type = "2"
)

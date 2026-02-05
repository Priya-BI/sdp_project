from pyspark import pipelines as dp
from pyspark.sql.functions import *



@dp.table
def sales_cura():
  df =  spark.read.table("sales_enriched")
#  df = df.withColumn("customer_sk", xxhash64(col("customer_id")))
  return df
  
  #withColumn('HashedID', sha2(concat_ws("||", col("customer_id")),256))

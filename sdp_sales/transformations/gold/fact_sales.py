from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.view
def fact_view():
    df_sale = spark.readStream.table("sales_enriched").alias("s")
    df_prod = spark.readStream.table("products_enriched").alias("p")
    df_store = spark.readStream.table("store_enriched").alias("st")

    df_join = (
        df_sale.join(df_prod, col("s.product_id") == col("p.product_id"), "inner")
              .select(
                  col("s.sales_id"),
                  col("s.customer_id"),
                  col("s.product_id"),
                  col("s.store_id"),
                  col("s.quantity"),
                  col("s.amount"),
                  col("s.sale_timestamp"),
                  col("s.total_amount"),
                  col("p.price").alias("product_price")
              ) 
           
    )

    return df_join

dp.create_streaming_table("fact_sales")
   
dp.create_auto_cdc_flow(
    target = "fact_sales",
    source = "fact_view",
    keys = ["sales_id"],
    sequence_by = "sale_timestamp",
    ignore_null_updates = True,
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = 1,
    track_history_column_list = None,
    track_history_except_column_list = None,
    name = None,
    once = False
)

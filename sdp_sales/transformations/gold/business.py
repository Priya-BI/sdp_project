from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.materialized_view(
    name="training2.sdp_sales.business_sales"
)
def business_sales():
    df_fact = spark.read.table("training2.sdp_sales.fact_sales").alias("f")
    df_dimcust = spark.read.table("training2.sdp_sales.dim_customers").alias("c")
    df_dimprod = spark.read.table("training2.sdp_sales.dim_products").alias("p")
    df_dimstore = spark.read.table("training2.sdp_sales.dim_store").alias("s")

    df_join = (
        df_fact.join(
            df_dimcust,
            df_fact.customer_id == df_dimcust.customer_id,
            "inner"
        ).join(
            df_dimprod,
            df_fact.product_id == df_dimprod.product_id,
            "inner"
        ).join(
            df_dimstore,
            df_fact.store_id == df_dimstore.store_id,
            "inner"
        )
    )    

    df_prun = df_join.select(
        "f.customer_id",
        "c.region",
        "s.store_id",
        "p.category",
        "f.total_amount")

    df_agg = (
        df_prun.groupBy(
            "region",
            "category"
        ).agg(
            {"total_amount": "sum"}
        ).withColumnRenamed(
            "sum(total_amount)",
            "total_sales"
        )
    )
    return df_agg

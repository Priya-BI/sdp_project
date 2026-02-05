from pyspark import pipelines as dp
from pyspark.sql.functions import *


dp.create_streaming_table(
    name="dim_products",
    comment="Product dimensional data"
    )

dp.create_auto_cdc_flow(
    target = "dim_products",
    source = "products_enriched",
    keys = ["product_id"],
    sequence_by = "last_updated",
    ignore_null_updates = True,
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = ["__START_AT", "__END_AT","price"],
    #stored_as_scd_type = 2,
    track_history_column_list = None,
    track_history_except_column_list = None,
    name = None,
    once = False
)

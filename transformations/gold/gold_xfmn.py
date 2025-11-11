import dlt
#gold data ingestion with SCD 2

#ingest Customers

dlt.create_streaming_table(
    name= "customers_dim_gold"
)

dlt.create_auto_cdc_flow(
    target = "customers_dim_gold",
    source = "customers_silver_view",
    keys = ["customer_id"],
    sequence_by = "last_updated",
    ignore_null_updates = False,
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = 2,
    track_history_column_list = None,
    track_history_except_column_list = None

)

# -------------------------------------------------------------------------
#ingest Customers

dlt.create_streaming_table(
    name= "products_dim_gold"
)

dlt.create_auto_cdc_flow(
    target = "products_dim_gold",
    source = "products_silver_view",
    keys = ["product_id"],
    sequence_by = "last_updated",
    ignore_null_updates = False,
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = 2,
    track_history_column_list = None,
    track_history_except_column_list = None

)

# ---------------------------------------------------------------------------------
#ingest Customers

dlt.create_streaming_table(
    name= "sales_fact_gold"
)

dlt.create_auto_cdc_flow(
    target = "sales_fact_gold",
    source = "sales_silver_view",
    keys = ["sales_id"],
    sequence_by = "sale_timestamp",
    ignore_null_updates = False,
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = 2,
    track_history_column_list = None,
    track_history_except_column_list = None

)
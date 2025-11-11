import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

"""load customer data into streaming table, but first, create a view with expectations met"""

# rules 

customer_rules = {
    "valid_id": "customer_id IS NOT NULL",
    "valid_customer_name":"customer_name IS NOT NULL",
    "valid_region": "region IN ('EAST','WEST','NORTH','SOUTH')"
}
#customer stream table

dlt.create_streaming_table(
    name = "customers_silver"
)

# create a view to transform silver data.
@dlt.view(
    name = "customers_silver_view",
    comment="validate records using exoectations"
)
@dlt.expect_all(customer_rules)

def customers_silver_view():
    return spark.readStream.table("learning_dlt_uc.bank_sales_schema.customers_bronze")

#auto CDC to save into streaming table

dlt.create_auto_cdc_flow(
    target = "customers_silver",
    source = "customers_silver_view",
    keys = ["customer_id"],
    sequence_by = "last_updated",
    ignore_null_updates = False,
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = 1,
    track_history_column_list = None,
    track_history_except_column_list = None

)

# ----------------------------------------------------------------------------------------
# REPEAT FOR PRODUCTS & SALES


product_rules = {
    "valid_id": "product_id IS NOT NULL",
    "valid_product_name":"product_name IS NOT NULL",
    "valid_price": "price >= 0"
}
#product stream table

dlt.create_streaming_table(
    name = "products_silver"
)

# create a view to transform silver data.
@dlt.view(
    name = "products_silver_view",
    comment="validate records using exoectations"
)
@dlt.expect_all(product_rules)

def products_silver_view():
    return spark.readStream.table("learning_dlt_uc.bank_sales_schema.products_bronze")

#auto CDC to save into streaming table

dlt.create_auto_cdc_flow(
    target = "products_silver",
    source = "products_silver_view",
    keys = ["product_id"],
    sequence_by = "last_updated",
    ignore_null_updates = False,
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = 1,
    track_history_column_list = None,
    track_history_except_column_list = None

)




# ----------------------------------------------------------------------------------------
# REPEAT FOR SALES


sales_rules = {
    "valid_sale": "sales_id IS NOT NULL",
    "valid_quantity":"quantity >= 0",
    "amount": "amount >= 0"
}
#product stream table

dlt.create_streaming_table(
    name = "sales_silver"
)

# create a view to transform silver data.
@dlt.view(
    name = "sales_silver_view",
    comment="validate records using exoectations"
)
@dlt.expect_all(sales_rules)

def sales_silver_view():
    return spark.readStream.table("learning_dlt_uc.bank_sales_schema.agg_sales_bronze")

#auto CDC to save into streaming table

dlt.create_auto_cdc_flow(
    target = "sales_silver",
    source = "sales_silver_view",
    keys = ["sales_id"],
    sequence_by = "sale_timestamp",
    ignore_null_updates = False,
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = 1,
    track_history_column_list = None,
    track_history_except_column_list = None

)

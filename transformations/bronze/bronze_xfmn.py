import dlt
from pyspark.sql.functions import *


# Ingest customers into a streaming table.

@dlt.table(
    name = "customers_bronze"
)

def customers_bronze():
    cust_data = spark.readStream.table("learning_dlt_uc.bank_sales_schema.customers_landing")
    cust_data = cust_data.withColumn("first_name", substring_index(col("customer_name"), " ", 1)).\
                withColumn("last_name", substring_index(col("customer_name"), " ", -1)).\
                withColumn("region", upper(col("region"))).\
                withColumn("ingestion_time", current_timestamp())
    return cust_data


#ingest product data

@dlt.table(
    name = "products_bronze",

)

def products_bronze():

    price_status = (
        when(col("price")>500, "HIGH").\
        when(col("price")>200, "MEDIUM").\
        otherwise("LOW")
    )
    product_data = spark.readStream.table("learning_dlt_uc.bank_sales_schema.products_landing").\
        withColumn("product_value",price_status).\
        withColumn("ingestion_time", current_timestamp()).\
        withColumn("category", upper(col("category")))
    return product_data

# ingest sales data from east and west into a streaming table

dlt.create_streaming_table(
    name = "agg_sales_bronze",
    comment = "Ingest and combine sales data from landing schema into a streaming table, with basic transformations"
)
@dlt.append_flow(target = "agg_sales_bronze")
def east_sales_bronze():
    data_east = spark.readStream.table("learning_dlt_uc.bank_sales_schema.sales_east_landing").\
        withColumn("total_amount", col("quantity")*col("amount")).\
        withColumn("region", lit("EAST")).\
        withColumn("ingestion_time", current_timestamp())
    return data_east

@dlt.append_flow(target = "agg_sales_bronze")
def west_sales_bronze():
    data_west = spark.readStream.table("learning_dlt_uc.bank_sales_schema.sales_west_landing").\
        withColumn("total_amount", col("quantity")*col("amount")).\
        withColumn("region", lit("WEST")).\
        withColumn("ingestion_time", current_timestamp())
    return data_west





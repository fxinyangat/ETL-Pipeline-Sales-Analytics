import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.table(
    name="customer_sales"
)

def customer_sales():
  df_sales = spark.readStream.table("sales_fact_gold")
  df_customers = spark.read.table("customers_dim_gold")
  df_cust_sales = df_sales.join(df_customers, df_sales.customer_id == df_customers.customer_id, "left")

  selected_cols = df_cust_sales.select(df_sales.region.alias("sales_region"), "product_id", "amount", "quantity","total_amount" )

  df_grp = selected_cols.groupBy("sales_region").agg(sum(col("total_amount").cast(FloatType())).alias("regional_sales"), sum("quantity").alias("regional_quantity"))

  return df_grp


@dlt.table(
    name = "product_cat_sales"
)

def product_cat_sales():
    df_sales = spark.readStream.table("sales_fact_gold")
    df_products = spark.read.table("products_dim_gold")
    df_prod_sales = df_sales.join(df_products, df_sales.product_id == df_products.product_id, "left")

    selected_cols = df_prod_sales.select("category", "product_name", "amount", "quantity","total_amount", "product_value", df_sales.region.alias("region"))

    df_grp = selected_cols.groupBy("category", "product_name").agg(sum("total_amount").alias("product_sales"), sum("quantity").alias("product_quantity"))

    return df_grp
  

@dlt.table(
    name = "region_category_sales"
)

def region_category_sales():
    df_sales = spark.readStream.table("sales_fact_gold")
    df_products = spark.read.table("products_dim_gold")
    df_prod_sales = df_sales.join(df_products, df_sales.product_id == df_products.product_id, "left")

    selected_cols = df_prod_sales.select("category", "product_name", "amount", "quantity","total_amount", "product_value", df_sales.region.alias("region"))

    df_grp = selected_cols.groupBy("category", "region").agg(sum("total_amount").alias("product_sales"), sum("quantity").alias("product_quantity"))

    return df_grp
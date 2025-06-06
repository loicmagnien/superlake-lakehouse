import pyspark.sql.types as T
import pyspark.sql.functions as F
from superlake.core import SuperDeltaTable, SuperDataframe, TableSaveMode
from lakehouse.ingestion.erp.erp_sales_transactions import get_pipeline_objects_erp_sales_transactions


def get_model_fact_sales(super_spark, catalog_name, logger, managed, superlake_dt):

    # gold table
    fact_sales = SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="04_dwh",
        table_name="fact_sales",
        table_schema=T.StructType([
            T.StructField("sales_key", T.StringType(), False, {"description": "Unique key of the sales"}),
            T.StructField("order_id", T.IntegerType(), False, {"description": "Unique ID of the order"}),
            T.StructField("order_line", T.IntegerType(), False, {"description": "Line number of the order"}),
            T.StructField("date_key", T.StringType(), False, {"description": "Unique key of the date"}),
            T.StructField("customer_key", T.StringType(), False, {"description": "Unique key of the customer"}),
            T.StructField("product_key", T.StringType(), False, {"description": "Unique key of the product"}),
            T.StructField("store_key", T.StringType(), False, {"description": "Unique key of the store"}),
            T.StructField("promo_key", T.StringType(), False, {"description": "Unique key of the promo"}),
            T.StructField("quantity", T.IntegerType(), False, {"description": "Quantity of the product"}),
            T.StructField("unit_price", T.DecimalType(10, 2), False, {"description": "Unit price of the product"}),
            T.StructField("discount", T.DecimalType(10, 2), False, {"description": "Discount of the product"}),
            T.StructField("cost", T.DecimalType(10, 2), False, {"description": "Cost of the product"}),
            T.StructField("total_price", T.DecimalType(10, 2), False, {"description": "Total price of the product"}),
            T.StructField("discount_pct", T.DecimalType(10, 2), False, {"description": "Discount percentage of the product"}),
            T.StructField("profit", T.DecimalType(10, 2), False, {"description": "Profit of the product"}),
            T.StructField("superlake_dt", T.TimestampType(), False, {"description": "Ingestion timestamp"})
        ]),
        table_save_mode=TableSaveMode.Merge,
        primary_keys=["sales_key"],
        partition_cols=[],
        pruning_partition_cols=True,
        pruning_primary_keys=False,
        optimize_table=True,
        logger=logger,
        managed=managed,
        foreign_keys=[
            {
                "fk_columns": ["date_key"],
                "ref_table": f"{catalog_name}.04_dwh.dim_date",
                "ref_columns": ["date_key"],
                "fk_name": None
            },
            {
                "fk_columns": ["customer_key"],
                "ref_table": f"{catalog_name}.04_dwh.dim_customer",
                "ref_columns": ["customer_key"],
                "fk_name": None
            },
            {
                "fk_columns": ["product_key"],
                "ref_table": f"{catalog_name}.04_dwh.dim_product",
                "ref_columns": ["product_key"],
                "fk_name": None
            },
            {
                "fk_columns": ["store_key"],
                "ref_table": f"{catalog_name}.04_dwh.dim_store",
                "ref_columns": ["store_key"],
                "fk_name": None
            },
            {
                "fk_columns": ["promo_key"],
                "ref_table": f"{catalog_name}.04_dwh.dim_promo",
                "ref_columns": ["promo_key"],
                "fk_name": None
            },
        ],
        table_description=(
            "Fact table for sales transactions. Each row represents a unique sales order line, including keys to date, customer, "
            "product, store, and promotion dimensions, as well as sales metrics such as quantity, unit price, discount, cost, "
            "total price, discount percentage, and profit. Sourced from ERP sales transactions."
        )
    )

    # model function
    def generate_fact_sales(super_spark, catalog_name, logger, managed, superlake_dt):
        # get the required pipeline objects
        (_, silver_erp_sales_transactions, _, _, _) = get_pipeline_objects_erp_sales_transactions(
            super_spark, catalog_name, logger, managed, superlake_dt
        )
        # read the silver table
        df = silver_erp_sales_transactions.read()
        # df = super_spark.spark.sql("SELECT * FROM 02_silver.erp_sales_transactions")
        df = df.withColumnRenamed("superlake_dt", "source_superlake_dt")
        df = df.withColumn("superlake_dt", F.lit(superlake_dt).cast(T.TimestampType()))
        # generate surrogate key for the fact table
        df = SuperDataframe(df).generate_surrogate_key(
            field_list=["order_id", "order_line"],
            key_column_name="sales_key"
        )
        # instead of calculating the 'foreign keys' we should use the surrogate keys from the dimensions
        df = SuperDataframe(df).generate_surrogate_key(
            field_list=["store_id", "store_name", "city", "state"],
            key_column_name="store_key"
        )
        df = SuperDataframe(df).generate_surrogate_key(
            field_list=["customer_id", "customer_name"],
            key_column_name="customer_key"
        )
        df = SuperDataframe(df).generate_surrogate_key(
            field_list=["product_id", "product_name", "category", "brand"],
            key_column_name="product_key"
        )
        df = SuperDataframe(df).generate_surrogate_key(
            field_list=["promo_code"],
            key_column_name="promo_key"
        )
        df = df.withColumn("date_key", F.date_format(F.col("order_date"), "yyyyMMdd"))
        # derivated facts
        df = df.withColumn("total_price", F.col("quantity") * F.col("unit_price"))
        df = df.withColumn(
            "discount_pct", F.round(100*F.col("discount") / F.col("unit_price"), 2)
        )
        df = df.withColumn("profit", F.col("total_price") - F.col("cost"))
        df = df.select(
            "sales_key",
            "date_key", "customer_key", "product_key", "store_key", "promo_key",
            "order_id", "order_line",
            "quantity", "unit_price", "discount", "cost",
            "total_price", "discount_pct", "profit",
            "superlake_dt"
        )
        return df

    return fact_sales, generate_fact_sales

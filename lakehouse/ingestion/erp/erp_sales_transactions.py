import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from superlake.core import SuperDeltaTable, TableSaveMode, SchemaEvolution


def get_pipeline_objects_erp_sales_transactions(super_spark, catalog_name, logger, managed, superlake_dt):

    # source schema
    source_erp_sales_transactions_schema = T.StructType([
        T.StructField("order_id", T.LongType(), False, {"description": "Unique ID of the order"}),
        T.StructField("order_line", T.IntegerType(), False, {"description": "Line number of the order"}),
        T.StructField("order_date", T.DateType(), True, {"description": "Date of the order"}),
        T.StructField("customer_id", T.StringType(), True, {"description": "Unique ID of the customer"}),
        T.StructField("customer_name", T.StringType(), True, {"description": "Name of the customer"}),
        T.StructField("product_id", T.StringType(), True, {"description": "Unique ID of the product"}),
        T.StructField("product_name", T.StringType(), True, {"description": "Name of the product"}),
        T.StructField("category", T.StringType(), True, {"description": "Category of the product"}),
        T.StructField("brand", T.StringType(), True, {"description": "Brand of the product"}),
        T.StructField("store_id", T.StringType(), True, {"description": "Unique ID of the store"}),
        T.StructField("store_name", T.StringType(), True, {"description": "Name of the store"}),
        T.StructField("city", T.StringType(), True, {"description": "City of the store"}),
        T.StructField("state", T.StringType(), True, {"description": "State of the store"}),
        T.StructField("quantity", T.IntegerType(), True, {"description": "Quantity of the product"}),
        T.StructField("unit_price", T.DecimalType(10, 2), True, {"description": "Unit price of the product"}),
        T.StructField("discount", T.DecimalType(10, 2), True, {"description": "Discount of the product"}),
        T.StructField("cost", T.DecimalType(10, 2), True, {"description": "Cost of the product"}),
        T.StructField("promo_code", T.StringType(), True, {"description": "Promo code of the order"}),
    ])

    # add superlake_dt to the schema for bronze and silver tables
    erp_sales_transactions_schema = source_erp_sales_transactions_schema.add(
        T.StructField("superlake_dt", T.TimestampType(), True, {"description": "Timestamp of the data ingestion"})
    )

    # bronze table
    bronze_erp_sales_transactions = SuperDeltaTable(
            super_spark=super_spark,
            catalog_name=catalog_name,
            schema_name="01_bronze",
            table_name="erp_sales_transactions",
            table_schema=erp_sales_transactions_schema,
            table_save_mode=TableSaveMode.Append,
            primary_keys=["order_id", "order_line"],
            partition_cols=["superlake_dt"],
            pruning_partition_cols=True,
            pruning_primary_keys=False,
            optimize_table=False,
            optimize_zorder_cols=[],
            optimize_target_file_size=100000000,
            compression_codec="snappy",
            schema_evolution_option=SchemaEvolution.Merge,
            logger=logger,
            managed=managed,
            table_description="Raw ERP sales transactions (generated data)"
        )

    # silver table
    silver_erp_sales_transactions = SuperDeltaTable(
            super_spark=super_spark,
            catalog_name=catalog_name,
            schema_name="02_silver",
            table_name="erp_sales_transactions",
            table_schema=erp_sales_transactions_schema,
            table_save_mode=TableSaveMode.Merge,
            primary_keys=["order_id", "order_line"],
            partition_cols=[],
            pruning_partition_cols=True,
            pruning_primary_keys=False,
            optimize_table=True,
            optimize_zorder_cols=[],
            optimize_target_file_size=100000000,
            compression_codec="snappy",
            schema_evolution_option=SchemaEvolution.Merge,
            logger=logger,
            delta_properties={
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true"
            },
            managed=managed,
            table_description="Cleaned ERP sales transactions (generated data)"
        )

    # cdc function
    def erp_sales_transactions_cdc(spark):
        # automatically set order line number (simulate CDC and generate data)
        if silver_erp_sales_transactions.table_exists(spark):
            order_line_max = (
                silver_erp_sales_transactions.read()
                .select(F.max("order_line"))
                .collect()[0][0]
            )
            if order_line_max is None:
                order_line_max = 1
            else:
                order_line_max += 1
        else:
            order_line_max = 1
        # generate data
        row_count = 100
        df = spark.range(row_count).withColumnRenamed("id", "order_id")
        df = (
            df.withColumn("order_line", F.lit(order_line_max))
            .withColumn("order_date", F.expr("current_date() - CAST(rand() * 90 AS INT)"))
            .withColumn("customer_id", F.concat(F.lit("CUST"), (F.floor(F.rand() * 10) + 1).cast("string")))
            .withColumn(
                "customer_name",
                F.concat(
                    F.lit("Customer "),
                    F.regexp_extract(F.col("customer_id"), r"CUST(\\d+)", 1),
                    F.lit(f" {order_line_max}")
                ).cast("string")
            )
            .withColumn("product_id", F.concat(F.lit("PROD"), (F.floor(F.rand() * 5) + 1).cast("string")))
            .withColumn("product_name", F.expr("""
                CASE
                    WHEN product_id = 'PROD1' THEN 'Smartphone'
                    WHEN product_id = 'PROD2' THEN 'Headphones'
                    WHEN product_id = 'PROD3' THEN 'Monitor'
                    WHEN product_id = 'PROD4' THEN 'Keyboard'
                    WHEN product_id = 'PROD5' THEN 'Mouse'
                    ELSE 'Unknown'
                END
            """))
            .withColumn("category", F.expr("""
                CASE
                    WHEN product_id = 'PROD1' THEN 'Main Device'
                    WHEN product_id = 'PROD2' THEN 'Accessory'
                    WHEN product_id = 'PROD3' THEN 'Display'
                    WHEN product_id = 'PROD4' THEN 'Accessory'
                    WHEN product_id = 'PROD5' THEN 'Accessory'
                    ELSE 'Unknown'
                END
            """))
            .withColumn("brand", F.expr(f"""
                CASE
                    WHEN product_id = 'PROD1' THEN 'BrandA {order_line_max}'
                    WHEN product_id = 'PROD2' THEN 'BrandA {order_line_max}'
                    WHEN product_id = 'PROD3' THEN 'BrandB'
                    WHEN product_id = 'PROD4' THEN 'BrandC'
                    WHEN product_id = 'PROD5' THEN 'BrandC'
                    ELSE 'Unknown'
                END
            """))
            .withColumn("store_id", F.concat(F.lit("STORE"), (F.floor(F.rand() * 3) + 1).cast("string")))
            .withColumn("store_name", F.expr(f"""
                CASE
                    WHEN store_id = 'STORE1' THEN 'New York Outlet {order_line_max}'
                    WHEN store_id = 'STORE2' THEN 'Chicago Branch'
                    WHEN store_id = 'STORE3' THEN 'SF Flagship'
                    ELSE 'Unknown'
                END
            """))
            .withColumn("city", F.expr("""
                CASE
                    WHEN store_id = 'STORE1' THEN 'New York'
                    WHEN store_id = 'STORE2' THEN 'Chicago'
                    WHEN store_id = 'STORE3' THEN 'San Francisco'
                    ELSE 'Unknown'
                END
            """))
            .withColumn("state", F.expr("""
                CASE
                    WHEN store_id = 'STORE1' THEN 'NY'
                    WHEN store_id = 'STORE2' THEN 'IL'
                    WHEN store_id = 'STORE3' THEN 'CA'
                    ELSE 'Unknown'
                END
            """))
            .withColumn("quantity", (F.floor(F.rand() * 5) + 1).cast("int"))
            .withColumn("unit_price", F.round(F.rand() * 900 + 100, 2))
            .withColumn("discount", F.round(F.rand() * 30, 2))
            .withColumn("cost", F.round(F.rand() * 500 + 50, 2))
            .withColumn("promo_code", F.expr(f"""
                CASE FLOOR(RAND() * 4)
                    WHEN 0 THEN 'OFFER{order_line_max}'
                    WHEN 1 THEN 'BLACKFRIDAY'
                    WHEN 2 THEN 'SUMMERSALE'
                    ELSE NULL
                END
            """))
        )
        return df

    # tra function
    def erp_sales_transactions_transform(df: DataFrame):
        return df

    # del function
    def erp_sales_transactions_delete(spark):
        return spark.createDataFrame([], source_erp_sales_transactions_schema)

    return (
        bronze_erp_sales_transactions,
        silver_erp_sales_transactions,
        erp_sales_transactions_cdc,
        erp_sales_transactions_transform,
        None
    )

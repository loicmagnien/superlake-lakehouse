import pyspark.sql.types as T
from superlake.core import SuperDeltaTable, TableSaveMode
from superlake.utils.modeling import SuperModeler
from lakehouse.ingestion.erp.erp_sales_transactions import get_pipeline_objects_erp_sales_transactions


def get_model_dim_product(super_spark, catalog_name, logger, managed, superlake_dt):
    dim_product = SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="04_dwh",
        table_name="dim_product",
        table_schema=T.StructType([
            T.StructField("product_key", T.StringType(), False, {"description": "Unique key of the product"}),
            T.StructField("product_id", T.StringType(), False, {"description": "Unique ID of the product"}),
            T.StructField("product_name", T.StringType(), False, {"description": "Name of the product"}),
            T.StructField("category", T.StringType(), False, {"description": "Category of the product"}),
            T.StructField("brand", T.StringType(), False, {"description": "Brand of the product"}),
            T.StructField("source_superlake_dt", T.TimestampType(), False, {"description": "Source ingestion timestamp"}),
            T.StructField("superlake_dt", T.TimestampType(), False, {"description": "Ingestion timestamp"})
        ]),
        table_save_mode=TableSaveMode.MergeSCD,
        primary_keys=["product_key"],
        scd_change_cols=["product_id", "product_name", "category", "brand"],
        partition_cols=[],
        pruning_partition_cols=True,
        pruning_primary_keys=False,
        optimize_table=True,
        logger=logger,
        managed=managed
    )

    def generate_dim_product(super_spark, catalog_name, logger, managed, superlake_dt):
        # get the required pipeline objects
        (_, silver_erp_sales_transactions, _, _, _) = get_pipeline_objects_erp_sales_transactions(
            super_spark, catalog_name, logger, managed, superlake_dt
        )
        # read the silver table
        return SuperModeler.generate_dimension(
            super_spark=super_spark,
            superlake_dt=superlake_dt,
            source_table=silver_erp_sales_transactions,
            source_columns=["product_id", "product_name", "category", "brand"],
            source_keys=["product_id"],
            sink_table=dim_product
        )

    return dim_product, generate_dim_product

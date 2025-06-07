import pyspark.sql.types as T
from superlake.core import SuperDeltaTable, TableSaveMode
from superlake.utils.modeling import SuperModeler
from lakehouse.ingestion.erp.erp_sales_transactions import get_pipeline_objects_erp_sales_transactions


def get_model_dim_store(super_spark, catalog_name, logger, managed, superlake_dt):
    dim_store = SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="03_gold",
        table_name="dim_store",
        table_schema=T.StructType([
            T.StructField("store_key", T.StringType(), False, {"description": "Unique key of the store"}),
            T.StructField("store_id", T.StringType(), False, {"description": "Unique ID of the store"}),
            T.StructField("store_name", T.StringType(), False, {"description": "Name of the store"}),
            T.StructField("city", T.StringType(), False, {"description": "City of the store"}),
            T.StructField("state", T.StringType(), False, {"description": "State of the store"}),
            T.StructField("source_superlake_dt", T.TimestampType(), False, {"description": "Source ingestion timestamp"}),
            T.StructField("superlake_dt", T.TimestampType(), False, {"description": "Ingestion timestamp"})
        ]),
        table_save_mode=TableSaveMode.MergeSCD,
        primary_keys=["store_key"],
        scd_change_cols=["store_id", "store_name", "city", "state"],
        partition_cols=[],
        pruning_partition_cols=True,
        pruning_primary_keys=False,
        optimize_table=True,
        logger=logger,
        managed=managed,
        table_description=(
            "Dimension table for stores. Each row represents a unique store, including its ID, name, city, and state. "
            "Used to join sales facts to store attributes for reporting and analysis."
        )
    )

    def generate_dim_store(super_spark, catalog_name, logger, managed, superlake_dt):
        # get the required pipeline objects
        (_, silver_erp_sales_transactions, _, _, _) = get_pipeline_objects_erp_sales_transactions(
            super_spark, catalog_name, logger, managed, superlake_dt
        )
        # read the silver table
        return SuperModeler.generate_dimension(
            super_spark=super_spark,
            superlake_dt=superlake_dt,
            source_table=silver_erp_sales_transactions,
            source_columns=["store_id", "store_name", "city", "state"],
            source_keys=["store_id"],
            sink_table=dim_store
        )

    return dim_store, generate_dim_store

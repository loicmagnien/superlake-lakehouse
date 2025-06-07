import pyspark.sql.types as T
from superlake.core import SuperDeltaTable, TableSaveMode
from superlake.utils.modeling import SuperModeler
from lakehouse.ingestion.erp.erp_sales_transactions import get_pipeline_objects_erp_sales_transactions


def get_model_dim_promo(super_spark, catalog_name, logger, managed, superlake_dt):
    dim_promo = SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="03_gold",
        table_name="dim_promo",
        table_schema=T.StructType([
            T.StructField("promo_key", T.StringType(), False, {"description": "Unique key of the promo"}),
            T.StructField("promo_code", T.StringType(), False, {"description": "Promo code"}),
            T.StructField("source_superlake_dt", T.TimestampType(), False, {"description": "Source ingestion timestamp"}),
            T.StructField("superlake_dt", T.TimestampType(), False, {"description": "Ingestion timestamp"})
        ]),
        table_save_mode=TableSaveMode.MergeSCD,
        primary_keys=["promo_key"],
        scd_change_cols=["promo_code"],
        partition_cols=[],
        pruning_partition_cols=True,
        pruning_primary_keys=False,
        optimize_table=True,
        logger=logger,
        managed=managed,
        table_description=(
            "Dimension table for promotions. Each row represents a unique promotion, including its promo code. "
            "Used to analyze the impact of promotions on sales."
        )
    )

    def generate_dim_promo(super_spark, catalog_name, logger, managed, superlake_dt):
        # get the required pipeline objects
        (_, silver_erp_sales_transactions, _, _, _) = get_pipeline_objects_erp_sales_transactions(
            super_spark, catalog_name, logger, managed, superlake_dt
        )
        # read the silver table
        return SuperModeler.generate_dimension(
            super_spark=super_spark,
            superlake_dt=superlake_dt,
            source_table=silver_erp_sales_transactions,
            source_columns=["promo_code"],
            source_keys=["promo_code"],
            sink_table=dim_promo
        )

    return dim_promo, generate_dim_promo

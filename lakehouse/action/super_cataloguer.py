from config import get_superlake_objects


if __name__ == "__main__":

    # ---------------------------------------------------
    #              Get superlake objects
    # ---------------------------------------------------

    config = get_superlake_objects()
    warehouse_dir = config['warehouse_dir']
    external_path = config['external_path']
    catalog_name = config['catalog_name']
    project_root = config['project_root']
    managed = config['managed']
    environment = config['environment']
    super_spark = config['super_spark']
    logger = config['logger']
    super_tracer = config['super_tracer']
    super_orchestrator = config['super_orchestrator']
    super_cataloguer = config['super_cataloguer']
    super_catalog_quality_table = config['super_catalog_quality_table']
    superlake_dt = config['superlake_dt']

    # ---------------------------------------------------
    #              Operations start here
    # ---------------------------------------------------

    # pre-create the trace and catalog quality tables
    super_tracer.generate_trace_table()
    super_catalog_quality_table.ensure_table_exists()

    # filter tables to apply operations on
    target_tables = [
        # dimensions
        "spark_catalog.03_gold.dim_bike_station",
        "spark_catalog.03_gold.dim_customer",
        "spark_catalog.03_gold.dim_date",
        "spark_catalog.03_gold.dim_product",
        "spark_catalog.03_gold.dim_promo",
        "spark_catalog.03_gold.dim_store",
        # facts
        "spark_catalog.03_gold.fact_sales",
        "spark_catalog.03_gold.fact_bike_status",
    ]

    # Apply table comments
    super_cataloguer.apply_table_comment(
        super_spark, catalog_name, logger, managed, superlake_dt,
        persist_catalog_quality=True,
        super_catalog_quality_table=super_catalog_quality_table,
        target_tables=target_tables
    )

    # Apply column comments
    super_cataloguer.apply_columns_comments(
        super_spark, catalog_name, logger, managed, superlake_dt,
        persist_catalog_quality=True,
        super_catalog_quality_table=super_catalog_quality_table,
        target_tables=target_tables
    )

    # Drop all foreign keys
    super_cataloguer.drop_foreign_keys(
        super_spark, catalog_name, logger, managed, superlake_dt,
        persist_catalog_quality=True,
        super_catalog_quality_table=super_catalog_quality_table,
        target_tables=target_tables
    )

    # Drop all primary keys
    super_cataloguer.drop_primary_keys(
        super_spark, catalog_name, logger, managed, superlake_dt,
        persist_catalog_quality=True,
        super_catalog_quality_table=super_catalog_quality_table,
        target_tables=target_tables
    )

    # Create all primary keys
    super_cataloguer.create_primary_keys(
        super_spark, catalog_name, logger, managed, superlake_dt,
        force_create_primary_keys=True,
        persist_catalog_quality=True,
        super_catalog_quality_table=super_catalog_quality_table,
        target_tables=target_tables
    )

    # Create all foreign keys
    super_cataloguer.create_foreign_keys(
        super_spark, catalog_name, logger, managed, superlake_dt,
        force_create_foreign_keys=True,
        persist_catalog_quality=True,
        super_catalog_quality_table=super_catalog_quality_table,
        target_tables=target_tables
    )

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

    # ---------------------------------------------------
    #   Example 1. Run a full load (all pipelines)
    # ---------------------------------------------------

    # Example 1. Run a full load (creates the tables on the fly)
    super_orchestrator.orchestrate(
        target_pipelines=[],
        direction='all',
        parallelize_pipelines=True,
        fail_fast=False,
        skip_downstream_on_failure=True,
    )

    # ---------------------------------------------------
    #   Example 2. Run a loop on a subset of pipelines
    # ---------------------------------------------------

    # filter pipelines to apply operations on
    target_pipelines = [
        "velib_station_status",
        "velib_station_info",
        "velov_station_status",
        "velov_station_info",
    ]

    # Run a 10 minutes loop on a subset of pipelines
    super_orchestrator.orchestrate(
        target_pipelines=target_pipelines,
        direction='none',
        parallelize_pipelines=True,
        fail_fast=False,
        skip_downstream_on_failure=True,
        loop_params={
            "min_interval_seconds": 60,
            "max_duration_seconds": 120
        }
    )

    # ---------------------------------------------------
    #   Example 3. process downstream pipelines
    # ---------------------------------------------------

    # Process downstream pipelines
    super_orchestrator.orchestrate(
        target_pipelines=["velib_station_status", "velov_station_status"],
        direction='downstream',
        parallelize_pipelines=True,
        fail_fast=True,
        skip_downstream_on_failure=True,
    )

    # ---------------------------------------------------
    #   Example 4. process upstream pipelines
    # ---------------------------------------------------

    # Process upstream pipelines
    super_orchestrator.orchestrate(
        target_pipelines=["fact_bike_status"],
        direction='upstream',
        parallelize_pipelines=True,
        fail_fast=False,
        skip_downstream_on_failure=True,
    )

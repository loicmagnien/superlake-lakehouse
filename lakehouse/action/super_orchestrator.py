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

    # Run a full load (creates the tables on the fly)
    super_orchestrator.orchestrate(
        loading_mode='file',
        orchestration_mode='process_first',
        target_pipelines=[],
        direction='all',
        parallelize_groups=True,
        fail_fast=False,
        skip_downstream_on_failure=True
    )

from datetime import datetime
from superlake.core.orchestration import SuperOrchestrator
from superlake.core import SuperSpark, SuperTracer
from superlake.monitoring import SuperLogger
import os


if __name__ == "__main__":

    # set the superlake parameters
    warehouse_dir = "./data/spark-warehouse"
    external_path = "./data/external-table/"
    catalog_name = "spark_catalog"
    managed = False
    environment = "prd"
    # this is the parent folder of the lakehouse folder
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))

    # display the parameters
    print("-----------------------------------------------------------------------------")
    print(f"project root: {project_root}")
    print(f"warehouse dir: {warehouse_dir}")
    print(f"external path: {external_path}")
    print(f"catalog name: {catalog_name}")
    print(f"managed: {managed}")
    print(f"environment: {environment}")
    print("-----------------------------------------------------------------------------")

    # create the superlake objects
    super_spark = SuperSpark(
        session_name="SuperSpark for SuperLake",
        warehouse_dir=warehouse_dir,
        external_path=external_path,
        catalog_name=catalog_name
    )
    logger = SuperLogger(name="SuperLake")
    super_tracer = SuperTracer(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="00_superlake",
        table_name="super_trace",
        managed=managed,
        logger=logger
    )

    # orchestrate the pipelines in process_first mode
    superlake_dt = datetime.now()
    orchestrator = SuperOrchestrator(
        super_spark=super_spark,
        catalog_name=catalog_name,
        logger=logger,
        managed=managed,
        superlake_dt=superlake_dt,
        super_tracer=super_tracer,
        environment=environment,
        project_root=project_root
    )
    orchestrator.orchestrate(
        loading_mode='file',
        orchestration_mode='process_first',
        target_pipelines=['fact_bike_status', 'dim_bike_station'],
        direction='all',
        parallelize_groups=True,
        fail_fast=False,
        skip_downstream_on_failure=True
    )

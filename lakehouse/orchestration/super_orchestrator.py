from datetime import datetime
from superlake.core.orchestration import SuperOrchestrator
from superlake.core import SuperSpark, SuperTracer
from superlake.monitoring import SuperLogger
from superlake.utils import SuperCataloguer
import os


if __name__ == "__main__":

    # set the superlake parameters

    # ------------------------------------ databricks hive metastore --------------------------------------
    # warehouse_dir is set with the value of spark.conf.get("spark.sql.warehouse.dir")
    warehouse_dir = "dbfs:/user/hive/warehouse"
    external_path = '/mnt/data/external-table/'
    catalog_name = "spark_catalog"

    # ------------------------------------------- unity catalog -------------------------------------------
    # warehouse_dir is usually spark.sql("SHOW EXTERNAL LOCATIONS").filter("name = 'metastore_default_location'").select("url").collect()[0][0]
    # external_path is a value of a existing external location that can be found with spark.sql("SHOW EXTERNAL LOCATIONS")
    warehouse_dir = "abfss://container@unity_catalog_storage_account.dfs.core.windows.net/UUID/"
    external_path = 'abfss://container@data_storage_account.dfs.core.windows.net/superlake/data/external-table/'
    catalog_name = "my_unity_catalog"

    # -------------------------------------------- local spark --------------------------------------------
    # on spark local, the warehouse_dir and the external_path can be set using absolute or relative paths
    warehouse_dir = "./data/spark-warehouse"
    external_path = "./data/external-table/"
    catalog_name = "spark_catalog"

    # this is the parent folder of the lakehouse folder
    # databricks
    project_root = "/Workspace/Repos/projects/superlake-lakehouse/lakehouse"
    # local spark
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))

    # table and environment management
    managed = False
    environment = "prd"

    # display the parameters
    print("-----------------------------------------------------------------------------")
    print(f"project root: {project_root}")
    print(f"warehouse dir: {warehouse_dir}")
    print(f"external path: {external_path}")
    print(f"catalog name: {catalog_name}")
    print(f"managed: {managed}")
    print(f"environment: {environment}")
    print("-----------------------------------------------------------------------------")

    # create the superspark
    super_spark = SuperSpark(
        session_name="SuperSpark for SuperLake",
        warehouse_dir=warehouse_dir,
        external_path=external_path,
        catalog_name=catalog_name
    )

    # create the superlogger
    logger = SuperLogger(name="SuperLake")

    # create the supertracer
    super_tracer = SuperTracer(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="00_superlake",
        table_name="super_trace",
        managed=managed,
        logger=logger
    )

    # set the superlake_dt
    superlake_dt = datetime.now()

    # create the orchestrator
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

    # orchestrate the pipelines for bikes
    orchestrator.orchestrate(
        loading_mode='file',
        orchestration_mode='process_first',
        target_pipelines=['fact_bike_status', 'dim_bike_station'],
        direction='all',
        parallelize_groups=False,
        fail_fast=False,
        skip_downstream_on_failure=True
    )

    # set the superlake_dt
    superlake_dt = datetime.now()

    # orchestrate all the pipelines
    orchestrator.orchestrate(
        loading_mode='file',
        orchestration_mode='process_first',
        target_pipelines=[],
        direction='all',
        parallelize_groups=False,
        fail_fast=False,
        skip_downstream_on_failure=True
    )

    # create the supercataloguer
    super_cataloguer = SuperCataloguer(project_root=project_root)

    # update all the tables descriptions and columns comments
    super_cataloguer.execute(
        super_spark=super_spark,
        catalog_name=catalog_name,
        logger=logger,
        managed=managed,
        superlake_dt=superlake_dt,
        register_tables=False,
        change_table_and_columns_comments=True,
    )

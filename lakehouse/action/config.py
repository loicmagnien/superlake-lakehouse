from os.path import abspath, dirname, join
from superlake.core import SuperSpark, SuperTracer
from superlake.core.orchestration import SuperOrchestrator
from superlake.monitoring import SuperLogger
from superlake.core.catalog import SuperCataloguer, SuperCatalogQualityTable
from datetime import datetime


def get_superlake_objects():

    # ------------------------------------ databricks hive metastore --------------------------------------
    # use this for databricks hive metastore
    warehouse_dir = "dbfs:/user/hive/warehouse"  # databricks hive metastore in the dbfs
    external_path = '/mnt/data/external-table/'  # databricks external mount point
    catalog_name = "spark_catalog"               # databricks "hive_metastore" alias is "spark_catalog"

    # ------------------------------------------- unity catalog -------------------------------------------
    # use this if your databricks setup is using unity catalog (spark.sql("SHOW EXTERNAL LOCATIONS"))
    warehouse_dir = "abfss://container@unity_catalog_storage_account.dfs.core.windows.net/UUID/"
    external_path = 'abfss://container@data_storage_account.dfs.core.windows.net/superlake/data/external-table/'
    catalog_name = "my_unity_catalog"

    # -------------------------------------------- local spark --------------------------------------------
    # use this for local spark setup
    warehouse_dir = "./data/spark-warehouse"
    external_path = "./data/external-table/"
    catalog_name = "spark_catalog"

    # this is the parent folder of the lakehouse folder
    # databricks repos
    project_root = "/Workspace/Repos/projects/superlake-lakehouse/lakehouse"
    # local setup
    project_root = abspath(join(dirname(__file__), '../'))

    # table and environment management
    managed = False
    environment = "prd"

    # Display the parameters
    print("-----------------------------------------------------------------------------", flush=True)
    print(f"project root:  {project_root}", flush=True)
    print(f"warehouse dir: {warehouse_dir}", flush=True)
    print(f"external path: {external_path}", flush=True)
    print(f"catalog name:  {catalog_name}", flush=True)
    print(f"managed:       {managed}", flush=True)
    print(f"environment:   {environment}", flush=True)
    print("-----------------------------------------------------------------------------", flush=True)

    # ---------------------------------------------------
    #              Instantiate super_spark
    # ---------------------------------------------------

    # Instantiate super_spark
    super_spark = SuperSpark(
        session_name="SuperSpark for SuperLake",
        warehouse_dir=warehouse_dir,
        external_path=external_path,
        catalog_name=catalog_name
    )

    # Instantiate superlogger
    logger = SuperLogger(name="SuperLake")

    # Instantiate super_tracer
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

    # Instantiate the super orchestrator
    super_orchestrator = SuperOrchestrator(
        super_spark=super_spark,
        catalog_name=catalog_name,
        logger=logger,
        managed=managed,
        superlake_dt=superlake_dt,
        super_tracer=super_tracer,
        environment=environment,
        project_root=project_root
    )

    # Instantiate the super_cataloguer
    super_cataloguer = SuperCataloguer(project_root=project_root)

    # Instantiate the super_catalog_quality_table
    super_catalog_quality_table = SuperCatalogQualityTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="00_superlake",
        table_name="super_catalog_quality",
        managed=managed,
        logger=logger if logger is not None else SuperLogger(name='DefaultLogger')
    )

    return {
        'warehouse_dir': warehouse_dir,
        'external_path': external_path,
        'catalog_name': catalog_name,
        'project_root': project_root,
        'managed': managed,
        'environment': environment,
        'super_spark': super_spark,
        'logger': logger,
        'super_tracer': super_tracer,
        'super_orchestrator': super_orchestrator,
        'super_cataloguer': super_cataloguer,
        'super_catalog_quality_table': super_catalog_quality_table,
        'superlake_dt': superlake_dt
    }

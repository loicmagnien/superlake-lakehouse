import pyspark.sql.types as T
from pyspark.sql import DataFrame
from superlake.core import SuperDeltaTable, TableSaveMode, SchemaEvolution
import pyspark.sql.functions as F
from lakehouse.utils.opendatasoft import load_opendatasoft_dataset_to_spark


def get_pipeline_objects_velib_station_status(super_spark, catalog_name, logger, managed, superlake_dt):
    # Source schema: keep raw coordonnees_geo
    source_velib_status_schema = T.StructType([
        T.StructField("stationcode", T.StringType(), True, {"description": "Unique ID of the station"}),
        T.StructField("name", T.StringType(), True, {"description": "Name of the station"}),
        T.StructField("is_installed", T.StringType(), True, {"description": "Whether the station is installed"}),
        T.StructField("numdocksavailable", T.IntegerType(), True, {"description": "Number of docks available"}),
        T.StructField("numbikesavailable", T.IntegerType(), True, {"description": "Number of bikes available"}),
        T.StructField("mechanical", T.IntegerType(), True, {"description": "Number of mechanical bikes available"}),
        T.StructField("ebike", T.IntegerType(), True, {"description": "Number of electric bikes available"}),
        T.StructField("is_renting", T.StringType(), True, {"description": "Whether the station is renting"}),
        T.StructField("is_returning", T.StringType(), True, {"description": "Whether the station is returning"}),
        T.StructField("duedate", T.StringType(), True, {"description": "Due date of the station"}),
        T.StructField("coordonnees_geo", T.StructType([
            T.StructField("lon", T.DoubleType(), True),
            T.StructField("lat", T.DoubleType(), True),
        ]), True, {"description": "Coordinates of the station"}),
        T.StructField("nom_arrondissement_communes", T.StringType(), True, {"description": "Arrondissement name"}),
        T.StructField("code_insee_commune", T.StringType(), True, {"description": "Code INSEE of the commune"}),
        T.StructField("station_opening_hours", T.StringType(), True, {"description": "Opening hours of the station"}),
        T.StructField("timestamp", T.TimestampType(), True, {"description": "Timestamp of the API call"}),
    ])

    bronze_velib_status_schema = T.StructType(
        source_velib_status_schema.fields + [
            T.StructField("superlake_dt", T.TimestampType(), True, {"description": "Timestamp of the data ingestion"})
        ]
    )

    silver_velib_status_schema = T.StructType([
        T.StructField("stationcode", T.StringType(), True, {"description": "Unique ID of the station"}),
        T.StructField("name", T.StringType(), True, {"description": "Name of the station"}),
        T.StructField("is_installed", T.BooleanType(), True, {"description": "Whether the station is installed"}),
        T.StructField("numdocksavailable", T.IntegerType(), True, {"description": "Number of docks available"}),
        T.StructField("numbikesavailable", T.IntegerType(), True, {"description": "Number of bikes available"}),
        T.StructField("mechanical", T.IntegerType(), True, {"description": "Number of mechanical bikes available"}),
        T.StructField("ebike", T.IntegerType(), True, {"description": "Number of electric bikes available"}),
        T.StructField("is_renting", T.BooleanType(), True, {"description": "Whether the station is renting"}),
        T.StructField("is_returning", T.BooleanType(), True, {"description": "Whether the station is returning"}),
        T.StructField("duedate", T.StringType(), True, {"description": "Due date of the station"}),
        T.StructField("longitude", T.DoubleType(), True, {"description": "Longitude of the station"}),
        T.StructField("latitude", T.DoubleType(), True, {"description": "Latitude of the station"}),
        T.StructField("nom_arrondissement_communes", T.StringType(), True, {"description": "Arrondissement name"}),
        T.StructField("code_insee_commune", T.StringType(), True, {"description": "Code INSEE of the commune"}),
        T.StructField("station_opening_hours", T.StringType(), True, {"description": "Opening hours of the station"}),
        T.StructField("timestamp", T.TimestampType(), True, {"description": "Timestamp of the API call"}),
        T.StructField("superlake_dt", T.TimestampType(), True, {"description": "Timestamp of the data ingestion"})
    ])

    bronze_velib_status = SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="01_bronze",
        table_name="velib_station_status",
        table_schema=bronze_velib_status_schema,
        table_save_mode=TableSaveMode.Append,
        primary_keys=["stationcode", "timestamp"],
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
        table_description="Raw Velib station status from Open Data Soft"
    )

    silver_velib_status = SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="02_silver",
        table_name="velib_station_status",
        table_schema=silver_velib_status_schema,
        table_save_mode=TableSaveMode.Merge,
        primary_keys=["stationcode", "timestamp"],
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
        table_description="Cleaned Velib station status from Open Data Soft with SCD type 2"
    )

    # CDC function: fetch from API and return as Spark DataFrame
    def velib_status_cdc(spark):
        domain = "opendata.paris.fr"
        dataset_id = "velib-disponibilite-en-temps-reel"
        df = load_opendatasoft_dataset_to_spark(
            domain=domain,
            dataset_id=dataset_id,
            spark=spark,
            schema=source_velib_status_schema,
            batch_size=100,
            select=(
                "stationcode,name,is_installed,numdocksavailable,"
                "numbikesavailable,mechanical,ebike,is_renting,is_returning,"
                "duedate,coordonnees_geo,nom_arrondissement_communes,"
                "code_insee_commune,station_opening_hours"
            ),
            order_by="numbikesavailable desc"
        )
        df = df.filter(F.col("stationcode").isNotNull())
        df = df.withColumn("timestamp", F.current_timestamp().cast(T.TimestampType()))
        df = df.select([f.name for f in source_velib_status_schema.fields])
        return df

    # Transform function: convert types and fields for silver
    def velib_status_transform(df: DataFrame):
        if "coordonnees_geo" in df.columns:
            df = df.withColumn("longitude", F.col("coordonnees_geo.lon").cast(T.DoubleType()))
            df = df.withColumn("latitude", F.col("coordonnees_geo.lat").cast(T.DoubleType()))
            df = df.drop("coordonnees_geo")
        for col in ["is_installed", "is_renting", "is_returning"]:
            if col in df.columns:
                df = df.withColumn(col, (F.col(col) == F.lit("OUI")).cast(T.BooleanType()))
        df = df.select([f.name for f in silver_velib_status_schema.fields])
        return df

    def velib_status_delete(spark):
        return spark.createDataFrame([], source_velib_status_schema)

    return (
        bronze_velib_status,
        silver_velib_status,
        velib_status_cdc,
        velib_status_transform,
        None
    )

import pyspark.sql.types as T
from pyspark.sql import DataFrame
from superlake.core import SuperDeltaTable, TableSaveMode, SchemaEvolution
from lakehouse.utils.opendatasoft import load_opendatasoft_dataset_to_spark
import pyspark.sql.functions as F


def get_pipeline_objects_velib_station_info(super_spark, catalog_name, logger, managed, superlake_dt):
    # source schema
    source_velib_station_info_schema = T.StructType([
        T.StructField("stationcode", T.StringType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("capacity", T.IntegerType(), True),
        T.StructField("coordonnees_geo", T.StructType([
            T.StructField("lon", T.DoubleType(), True),
            T.StructField("lat", T.DoubleType(), True),
        ]), True),
        T.StructField("station_opening_hours", T.StringType(), True),
    ])

    # add superlake_dt to the schema for bronze and silver tables
    bronze_velib_station_info_schema = T.StructType(
        source_velib_station_info_schema.fields + [
            T.StructField("superlake_dt", T.TimestampType(), True, {"description": "Timestamp of the data ingestion"})
        ]
    )

    silver_velib_station_info_schema = T.StructType([
        T.StructField("stationcode", T.StringType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("capacity", T.IntegerType(), True),
        T.StructField("longitude", T.DoubleType(), True),
        T.StructField("latitude", T.DoubleType(), True),
        T.StructField("station_opening_hours", T.StringType(), True),
    ])

    # bronze table
    bronze_velib_station_info = SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="01_bronze",
        table_name="velib_station_info",
        table_schema=bronze_velib_station_info_schema,
        table_save_mode=TableSaveMode.Append,
        primary_keys=["stationcode"],
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
        table_description="Raw Velib station info from Open Data Soft"
    )

    # silver table
    silver_velib_station_info = SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="02_silver",
        table_name="velib_station_info",
        table_schema=silver_velib_station_info_schema,
        table_save_mode=TableSaveMode.MergeSCD,
        primary_keys=["stationcode"],
        scd_change_cols=["name", "capacity", "longitude", "latitude", "station_opening_hours"],
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
        table_description="Cleaned Velib station info from Open Data Soft with SCD type 2"
    )

    # cdc function
    def velib_station_info_cdc(spark):
        try:
            domain = "opendata.paris.fr"
            dataset_id = "velib-emplacement-des-stations"
            df = load_opendatasoft_dataset_to_spark(
                domain=domain,
                dataset_id=dataset_id,
                spark=spark,
                schema=source_velib_station_info_schema,
                batch_size=100,
                select="stationcode,name,capacity,coordonnees_geo,station_opening_hours"
            )
            df = df.filter(F.col("stationcode").isNotNull())
            df = df.withColumn("timestamp", F.current_timestamp().cast(T.TimestampType()))
            df = df.select([f.name for f in source_velib_station_info_schema.fields])
        except Exception:
            logger.error("Could not retrieve the data.")
            df = spark.createDataFrame([], source_velib_station_info_schema)
        return df

    # tra function
    def velib_station_info_transform(df: DataFrame):
        if "coordonnees_geo" in df.columns:
            df = df.withColumn("longitude", F.col("coordonnees_geo.lon").cast(T.DoubleType()))
            df = df.withColumn("latitude", F.col("coordonnees_geo.lat").cast(T.DoubleType()))
            df = df.drop("coordonnees_geo")
        return df

    # del function (no delete logic, returns empty DataFrame)
    def velib_station_info_delete(spark):
        return spark.createDataFrame([], source_velib_station_info_schema)

    return (
        bronze_velib_station_info,
        silver_velib_station_info,
        velib_station_info_cdc,
        velib_station_info_transform,
        None
    )

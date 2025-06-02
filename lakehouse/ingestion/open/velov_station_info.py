import pyspark.sql.types as T
from pyspark.sql import DataFrame
from superlake.core import SuperDeltaTable, TableSaveMode, SchemaEvolution
import pyspark.sql.functions as F
import requests


def get_pipeline_objects_velov_station_info(super_spark, catalog_name, logger, managed, superlake_dt):
    # Source schema (as per API fields)
    source_velov_station_info_schema = T.StructType([
        T.StructField("station_id", T.IntegerType(), True, {"description": "Unique ID of the station"}),
        T.StructField("name", T.StringType(), True, {"description": "Name of the station"}),
        T.StructField("lat", T.DoubleType(), True, {"description": "Latitude"}),
        T.StructField("lon", T.DoubleType(), True, {"description": "Longitude"}),
        T.StructField("address", T.StringType(), True, {"description": "Address of the station"}),
        T.StructField("rental_methods", T.StringType(), True, {"description": "Rental methods (comma-separated)"}),
        T.StructField("capacity", T.IntegerType(), True, {"description": "Capacity of the station"}),
        T.StructField("last_updated", T.LongType(), True, {"description": "Last updated timestamp (epoch)"}),
    ])

    # Bronze schema: add the superlake_dt column
    bronze_velov_station_info_schema = T.StructType(
        source_velov_station_info_schema.fields + [
            T.StructField("superlake_dt", T.TimestampType(), True, {"description": "Timestamp of the data ingestion"})
        ]
    )

    # Silver schema: convert last_updated to timestamp
    silver_velov_station_info_schema = T.StructType([
        T.StructField("station_id", T.IntegerType(), True, {"description": "Unique ID of the station"}),
        T.StructField("name", T.StringType(), True, {"description": "Name of the station"}),
        T.StructField("lat", T.DoubleType(), True, {"description": "Latitude"}),
        T.StructField("lon", T.DoubleType(), True, {"description": "Longitude"}),
        T.StructField("address", T.StringType(), True, {"description": "Address of the station"}),
        T.StructField("rental_methods", T.StringType(), True, {"description": "Rental methods (comma-separated)"}),
        T.StructField("capacity", T.IntegerType(), True, {"description": "Capacity of the station"}),
        T.StructField("last_updated", T.TimestampType(), True, {"description": "Last updated timestamp"}),
        T.StructField("superlake_dt", T.TimestampType(), True, {"description": "Timestamp of the data ingestion"})
    ])

    # Bronze table
    bronze_velov_station_info = SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="01_bronze",
        table_name="velov_station_info",
        table_schema=bronze_velov_station_info_schema,
        table_save_mode=TableSaveMode.Append,
        primary_keys=["station_id", "last_updated"],
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
        table_description="Raw Velov station info from Grand Lyon"
    )

    # Silver table
    silver_velov_station_info = SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="02_silver",
        table_name="velov_station_info",
        table_schema=silver_velov_station_info_schema,
        table_save_mode=TableSaveMode.MergeSCD,
        primary_keys=["station_id"],
        scd_change_cols=[
            "name", "lat", "lon", "address", "rental_methods", "capacity"
        ],
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
        table_description="Cleaned Velov station info from Grand Lyon with SCD type 2"
    )

    # CDC function: fetch from API and return as Spark DataFrame
    def velov_station_info_cdc(spark):
        info_url = 'https://download.data.grandlyon.com/files/rdata/jcd_jcdecaux.jcdvelov/station_information.json'
        response = requests.get(info_url)
        data = response.json()
        last_updated = int(data['last_updated'])
        stations = data['data']['stations']
        rows = [
            (
                int(station['station_id']),
                station['name'],
                float(station['lat']),
                float(station['lon']),
                station['address'],
                ','.join(station.get('rental_methods', [])),
                int(station['capacity']),
                last_updated
            )
            for station in stations
        ]
        df = spark.createDataFrame(rows, schema=source_velov_station_info_schema)
        return df

    # Transform function: convert last_updated to timestamp
    def velov_station_info_transform(df: DataFrame):
        if "last_updated" in df.columns:
            df = df.withColumn("last_updated", F.from_unixtime(F.col("last_updated")).cast(T.TimestampType()))
            df = df.filter(F.col("last_updated") >= F.lit("1900-01-01 00:00:00"))
        df = df.select([f.name for f in silver_velov_station_info_schema.fields])
        return df

    # Delete function (returns empty DataFrame)
    def velov_station_info_delete(spark):
        return spark.createDataFrame([], source_velov_station_info_schema)

    return (
        bronze_velov_station_info,
        silver_velov_station_info,
        velov_station_info_cdc,
        velov_station_info_transform,
        None
    )

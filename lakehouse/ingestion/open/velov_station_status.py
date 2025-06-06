import pyspark.sql.types as T
from pyspark.sql import DataFrame
from superlake.core import SuperDeltaTable, TableSaveMode, SchemaEvolution
import pyspark.sql.functions as F
import requests


def get_pipeline_objects_velov_station_status(super_spark, catalog_name, logger, managed, superlake_dt):
    # Source schema (as per API fields)
    source_velov_station_status_schema = T.StructType([
        T.StructField("station_id", T.IntegerType(), True, {"description": "Unique ID of the station"}),
        T.StructField("last_updated", T.LongType(), True, {"description": "Last updated timestamp (epoch)"}),
        T.StructField("num_bikes_available", T.IntegerType(), True, {"description": "Number of bikes available"}),
        T.StructField("num_bikes_disabled", T.IntegerType(), True, {"description": "Number of bikes disabled"}),
        T.StructField("num_docks_available", T.IntegerType(), True, {"description": "Number of docks available"}),
        T.StructField("num_docks_disabled", T.IntegerType(), True, {"description": "Number of docks disabled"}),
        T.StructField("is_installed", T.IntegerType(), True, {"description": "Whether the station is installed (1/0)"}),
        T.StructField("is_renting", T.IntegerType(), True, {"description": "Whether the station is renting (1/0)"}),
        T.StructField("is_returning", T.IntegerType(), True, {"description": "Whether the station is returning (1/0)"}),
        T.StructField("last_reported", T.LongType(), True, {"description": "Last reported timestamp (epoch)"}),
    ])

    # Bronze schema: add the superlake_dt column
    bronze_velov_station_status_schema = T.StructType(
        source_velov_station_status_schema.fields + [
            T.StructField("superlake_dt", T.TimestampType(), True, {"description": "Timestamp of the data ingestion"})
        ]
    )

    # Silver schema: convert ints to booleans for is_installed, is_renting, is_returning, and add datetime columns
    silver_velov_station_status_schema = T.StructType([
        T.StructField("station_id", T.IntegerType(), True, {"description": "Unique ID of the station"}),
        T.StructField("last_updated", T.TimestampType(), True, {"description": "Last updated timestamp"}),
        T.StructField("num_bikes_available", T.IntegerType(), True, {"description": "Number of bikes available"}),
        T.StructField("num_bikes_disabled", T.IntegerType(), True, {"description": "Number of bikes disabled"}),
        T.StructField("num_docks_available", T.IntegerType(), True, {"description": "Number of docks available"}),
        T.StructField("num_docks_disabled", T.IntegerType(), True, {"description": "Number of docks disabled"}),
        T.StructField("is_installed", T.BooleanType(), True, {"description": "Whether the station is installed"}),
        T.StructField("is_renting", T.BooleanType(), True, {"description": "Whether the station is renting"}),
        T.StructField("is_returning", T.BooleanType(), True, {"description": "Whether the station is returning"}),
        T.StructField("last_reported", T.TimestampType(), True, {"description": "Last reported timestamp"}),
        T.StructField("superlake_dt", T.TimestampType(), True, {"description": "Timestamp of the data ingestion"})
    ])

    # Bronze table
    bronze_velov_station_status = SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="01_bronze",
        table_name="velov_station_status",
        table_schema=bronze_velov_station_status_schema,
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
        table_description="Raw Velov station status from Grand Lyon"
    )

    # Silver table
    silver_velov_station_status = SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="02_silver",
        table_name="velov_station_status",
        table_schema=silver_velov_station_status_schema,
        table_save_mode=TableSaveMode.Merge,
        primary_keys=["station_id", "last_updated"],
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
        table_description="Cleaned Velov station status from Grand Lyon with SCD type 2"
    )

    # CDC function: fetch from API and return as Spark DataFrame
    def velov_station_status_cdc(spark):
        status_url = 'https://download.data.grandlyon.com/files/rdata/jcd_jcdecaux.jcdvelov/station_status.json'
        response = requests.get(status_url)
        data = response.json()
        last_updated = int(data['last_updated'])
        stations = data['data']['stations']
        # Prepare rows for DataFrame, casting to correct types
        rows = [
            (
                int(s['station_id']),
                int(last_updated),
                int(s['num_bikes_available']),
                int(s['num_bikes_disabled']),
                int(s['num_docks_available']),
                int(s['num_docks_disabled']),
                int(s['is_installed']),
                int(s['is_renting']),
                int(s['is_returning']),
                int(s['last_reported'])
            )
            for s in stations
        ]
        df = spark.createDataFrame(rows, schema=source_velov_station_status_schema)
        df = df.filter(F.col("station_id").isNotNull())
        return df

    # Transform function: convert ints to booleans and epoch to timestamps
    def velov_station_status_transform(df: DataFrame):
        for col in ["is_installed", "is_renting", "is_returning"]:
            if col in df.columns:
                df = df.withColumn(col, (F.col(col) == F.lit(1)))
        # Convert epoch columns to timestamps
        for col in ["last_updated", "last_reported"]:
            if col in df.columns:
                df = df.withColumn(col, F.from_unixtime(F.col(col)).cast(T.TimestampType()))
        # Filter out ancient timestamps (before 1900-01-01)
        for col in ["last_updated", "last_reported"]:
            if col in df.columns:
                df = df.filter(F.col(col) >= F.lit("1900-01-01 00:00:00"))
        # Select columns for silver
        df = df.select([f.name for f in silver_velov_station_status_schema.fields])
        return df

    # Delete function (returns empty DataFrame)
    def velov_station_status_delete(spark):
        return spark.createDataFrame([], source_velov_station_status_schema)

    return (
        bronze_velov_station_status,
        silver_velov_station_status,
        velov_station_status_cdc,
        velov_station_status_transform,
        None
    )

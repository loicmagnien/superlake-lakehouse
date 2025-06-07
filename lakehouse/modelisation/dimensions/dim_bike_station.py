import pyspark.sql.types as T
import pyspark.sql.functions as F
from superlake.core import SuperDeltaTable, TableSaveMode, SchemaEvolution, SuperDataframe
from lakehouse.ingestion.open.velib_station_info import get_pipeline_objects_velib_station_info
from lakehouse.ingestion.open.velov_station_info import get_pipeline_objects_velov_station_info


def get_model_dim_bike_station(super_spark, catalog_name, logger, managed, superlake_dt):
    dim_bike_station = SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="03_gold",
        table_name="dim_bike_station",
        table_schema=T.StructType([
            T.StructField("station_key", T.StringType(), False, {"description": "Unique key of the station"}),
            T.StructField("station_source", T.StringType(), True, {"description": "Source of the station"}),
            T.StructField("station_id", T.StringType(), True, {"description": "Unique ID of the station"}),
            T.StructField("station_name", T.StringType(), True, {"description": "Name of the station"}),
            T.StructField("capacity", T.IntegerType(), True, {"description": "Capacity of the station"}),
            T.StructField("station_opening_hours", T.StringType(), True, {"description": "Opening hours of the station"}),
            T.StructField("longitude", T.DoubleType(), True, {"description": "Longitude of the station"}),
            T.StructField("latitude", T.DoubleType(), True, {"description": "Latitude of the station"}),
            T.StructField("address", T.StringType(), True, {"description": "Address of the station"}),
            T.StructField("source_superlake_dt", T.TimestampType(), False, {"description": "Source ingestion timestamp"}),
            T.StructField("superlake_dt", T.TimestampType(), False, {"description": "Ingestion timestamp"})
        ]),
        table_save_mode=TableSaveMode.MergeSCD,
        schema_evolution_option=SchemaEvolution.Merge,
        primary_keys=["station_key"],
        scd_change_cols=[
            "station_name", "capacity", "longitude", "latitude", "station_opening_hours", "address"
        ],
        partition_cols=[],
        pruning_partition_cols=True,
        pruning_primary_keys=False,
        optimize_table=True,
        logger=logger,
        managed=managed,
        table_description=(
            "Dimension table for bike stations. Each row represents a unique bike station, including its source, ID, name, "
            "capacity, location, and address. Used to join bike facts to station attributes."
        )
    )

    def generate_dim_bike_station(super_spark, catalog_name, logger, managed, superlake_dt):
        # Get silver tables
        (_, silver_velib_station_info, _, _, _) = get_pipeline_objects_velib_station_info(
            super_spark, catalog_name, logger, managed, superlake_dt
        )
        (_, silver_velov_station_info, _, _, _) = get_pipeline_objects_velov_station_info(
            super_spark, catalog_name, logger, managed, superlake_dt
        )

        # --- Velib ---
        # get the latest info for each station
        velib_df = (
            silver_velib_station_info.read()
            .filter(F.col("scd_is_current"))
            .select(
                F.lit('velib').alias("station_source"),
                F.col("stationcode").cast(T.StringType()).alias("station_id"),
                F.col("name").cast(T.StringType()).alias("station_name"),
                F.col("capacity"),
                F.col("longitude"),
                F.col("latitude"),
                F.col("station_opening_hours"),
                F.lit(None).cast(T.StringType()).alias("address"),
                F.col("superlake_dt").alias("source_superlake_dt"),
            )
        )

        # --- Velov ---
        velov_df = (
            silver_velov_station_info.read()
            .filter(F.col("scd_is_current"))
            .select(
                F.lit('velov').alias("station_source"),
                F.col("station_id").cast(T.StringType()).alias("station_id"),
                F.col("name").cast(T.StringType()).alias("station_name"),
                F.col("capacity"),
                F.col("lon").alias("longitude"),
                F.col("lat").alias("latitude"),
                F.lit(None).cast(T.StringType()).alias("station_opening_hours"),
                F.col("address"),
                F.col("superlake_dt").alias("source_superlake_dt"),
            )
        )

        # Union both
        unified_df = velib_df.unionByName(velov_df)

        # Generate the station_key
        unified_df = SuperDataframe(unified_df).generate_surrogate_key(
            field_list=["station_source", "station_id"],
            key_column_name="station_key"
        )

        # add the superlake_dt column
        unified_df = unified_df.withColumn("superlake_dt", F.current_timestamp())

        # Lowercase the station_name
        unified_df = (
            unified_df
            .withColumn("station_name", F.lower(F.col("station_name")))
            .withColumn("address", F.lower(F.col("address")))
        )

        # select the columns from the dim_bike_station table schema
        final_df = unified_df.select([f.name for f in dim_bike_station.table_schema.fields])
        return final_df

    return dim_bike_station, generate_dim_bike_station

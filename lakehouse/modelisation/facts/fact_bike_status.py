import pyspark.sql.types as T
import pyspark.sql.functions as F
from superlake.core import SuperDeltaTable, TableSaveMode, SuperDataframe
from lakehouse.ingestion.open.velib_station_status import get_pipeline_objects_velib_station_status
from lakehouse.ingestion.open.velov_station_status import get_pipeline_objects_velov_station_status


def get_model_fact_bike_status(super_spark, catalog_name, logger, managed, superlake_dt):
    fact_bike_status = SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="04_dwh",
        table_name="fact_bike_status",
        table_schema=T.StructType([
            T.StructField("station_key", T.StringType(), False, {"description": "Unique key of the station"}),
            T.StructField("date_key", T.StringType(), False, {"description": "Unique key of the date"}),
            T.StructField("station_source", T.StringType(), False, {"description": "Source of the station"}),
            T.StructField("station_id", T.StringType(), False, {"description": "Unique ID of the station"}),
            T.StructField("timestamp", T.TimestampType(), False, {"description": "Timestamp of the API call"}),
            T.StructField("available_docks", T.IntegerType(), False, {"description": "Number of docks available"}),
            T.StructField("available_bikes", T.IntegerType(), False, {"description": "Number of bikes available"}),
            T.StructField("mechanical_bikes", T.IntegerType(), True, {"description": "Number of mechanical bikes available"}),
            T.StructField("electric_bikes", T.IntegerType(), True, {"description": "Number of electric bikes available"}),
            T.StructField("is_installed", T.BooleanType(), True, {"description": "Whether the station is installed"}),
            T.StructField("is_renting", T.BooleanType(), True, {"description": "Whether the station is renting"}),
            T.StructField("is_returning", T.BooleanType(), True, {"description": "Whether the station is returning"}),
            T.StructField("due_date", T.TimestampType(), True, {"description": "Due date of the station"}),
            T.StructField("source_superlake_dt", T.TimestampType(), False, {"description": "Source ingestion timestamp"}),
            T.StructField("superlake_dt", T.TimestampType(), False, {"description": "Ingestion timestamp"})
        ]),
        table_save_mode=TableSaveMode.Merge,
        primary_keys=["station_key", "timestamp"],
        partition_cols=[],
        pruning_partition_cols=True,
        pruning_primary_keys=False,
        optimize_table=True,
        logger=logger,
        managed=managed,
        table_description=(
            "Fact table for bike station status. Each row represents a snapshot of a bike station's status at a specific timestamp, "
            "including available docks, available bikes, mechanical and electric bikes, and operational flags. Includes keys to "
            "station and date dimensions. Sourced from Velib and Velov station status APIs."
        )
    )

    def generate_fact_bike_status(super_spark, catalog_name, logger, managed, superlake_dt):
        spark = super_spark.spark
        # get the silver tables
        (_, silver_velib_station_status, _, _, _) = get_pipeline_objects_velib_station_status(
            super_spark, catalog_name, logger, managed, superlake_dt
        )
        (_, silver_velov_station_status, _, _, _) = get_pipeline_objects_velov_station_status(
            super_spark, catalog_name, logger, managed, superlake_dt
        )

        # get the max_source_superlake_dt for both velib and velov already present in the fact_bike_status table
        if fact_bike_status.table_exists(spark):
            max_velib_source_superlake_dt = (
                fact_bike_status.read()
                .filter(F.col("station_source") == "velib")
                .select(F.max("source_superlake_dt"))
                .collect()[0][0]
            )
            if max_velib_source_superlake_dt is None:
                max_velib_source_superlake_dt = F.lit("1970-01-01 00:00:00").cast("timestamp")
            max_velov_source_superlake_dt = (
                fact_bike_status.read()
                .filter(F.col("station_source") == "velov")
                .select(F.max("source_superlake_dt"))
                .collect()[0][0]
            )
            if max_velov_source_superlake_dt is None:
                max_velov_source_superlake_dt = F.lit("1970-01-01 00:00:00").cast("timestamp")
        else:
            max_velib_source_superlake_dt = F.lit("1970-01-01 00:00:00").cast("timestamp")
            max_velov_source_superlake_dt = F.lit("1970-01-01 00:00:00").cast("timestamp")

        # Velib
        velib_df = (
            silver_velib_station_status.read()
            .filter(F.col("superlake_dt") > max_velib_source_superlake_dt)
            .select(
                F.lit('velib').alias("station_source"),
                F.col("stationcode").alias("station_id"),
                F.col("timestamp"),
                F.col("numdocksavailable").alias("available_docks"),
                F.col("numbikesavailable").alias("available_bikes"),
                F.col("mechanical").alias("mechanical_bikes"),
                F.col("ebike").alias("electric_bikes"),
                F.col("is_installed").cast("boolean"),
                F.col("is_renting").cast("boolean"),
                F.col("is_returning").cast("boolean"),
                F.col("duedate").cast("timestamp").alias("due_date"),
                F.col("superlake_dt").alias("source_superlake_dt"),
            )
        )

        # Velov
        velov_df = (
            silver_velov_station_status.read()
            .filter(F.col("superlake_dt") > max_velov_source_superlake_dt)
            .select(
                F.lit('velov').alias("station_source"),
                F.col("station_id").cast("string").alias("station_id"),
                F.col("last_updated").alias("timestamp"),
                F.col("num_docks_available").alias("available_docks"),
                F.col("num_bikes_available").alias("available_bikes"),
                F.lit(None).cast("integer").alias("mechanical_bikes"),
                F.lit(None).cast("integer").alias("electric_bikes"),
                F.col("is_installed").cast("boolean"),
                F.col("is_renting").cast("boolean"),
                F.col("is_returning").cast("boolean"),
                F.lit(None).cast("timestamp").alias("due_date"),
                F.col("superlake_dt").alias("source_superlake_dt"),
            )
        )

        # Union
        df = velib_df.unionByName(velov_df)

        # Generate surrogate keys
        df = SuperDataframe(df).generate_surrogate_key(
            field_list=["station_source", "station_id"],
            key_column_name="station_key"
        )
        df = df.withColumn("date_key", F.date_format(F.col("timestamp"), "yyyyMMdd"))

        # add the superlake_dt column
        df = df.withColumn("superlake_dt", F.lit(superlake_dt).cast(T.TimestampType()))

        # Select columns in the right order
        fact_df = df.select([f.name for f in fact_bike_status.table_schema.fields])
        return fact_df

    return fact_bike_status, generate_fact_bike_status

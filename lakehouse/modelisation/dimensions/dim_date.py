import pyspark.sql.types as T
import pyspark.sql.functions as F
from datetime import date
from superlake.core import SuperDeltaTable, TableSaveMode


def get_model_dim_date(super_spark, catalog_name, logger, managed, superlake_dt):
    dim_date = SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="03_gold",
        table_name="dim_date",
        table_schema=T.StructType([
            T.StructField("date_key", T.StringType(), False, {"description": "Unique key of the date"}),
            T.StructField("date", T.DateType(), False, {"description": "Date"}),
            T.StructField("year", T.IntegerType(), False, {"description": "Year"}),
            T.StructField("month", T.IntegerType(), False, {"description": "Month"}),
            T.StructField("day", T.IntegerType(), False, {"description": "Day"}),
            T.StructField("week", T.IntegerType(), False, {"description": "Week"}),
            T.StructField("quarter", T.IntegerType(), False, {"description": "Quarter"}),
            T.StructField("day_of_week", T.IntegerType(), False, {"description": "Day of the week"}),
            T.StructField("is_weekend", T.BooleanType(), False, {"description": "Whether the date is a weekend"}),
            T.StructField("superlake_dt", T.TimestampType(), False, {"description": "Ingestion timestamp"})
        ]),
        table_save_mode=TableSaveMode.Merge,
        primary_keys=["date_key"],
        partition_cols=[],
        pruning_partition_cols=True,
        pruning_primary_keys=False,
        optimize_table=True,
        logger=logger,
        managed=managed,
        table_description=(
            "Date dimension table. Each row represents a unique calendar date, including year, month, day, week, quarter, "
            "day of week, and weekend flag. Used for time-based analysis and reporting."
        )
    )

    def generate_dim_date(super_spark, catalog_name, logger, managed, superlake_dt):
        spark = super_spark.spark
        # format the date range
        min_date = date(1970, 1, 1)
        max_date = date.today()
        start_date = date(min_date.year, min_date.month, min_date.day)
        end_date = date(max_date.year, max_date.month, max_date.day)
        num_days = (end_date - start_date).days
        date_range_df = spark.range(0, num_days).withColumn(
            "date", F.expr(f"date_add('{start_date}', CAST(id AS INT))")
        )
        date_range_df = date_range_df.withColumn("date_key", F.date_format(F.col("date"), "yyyyMMdd"))
        date_range_df = date_range_df.select("date_key", "date")
        date_range_df = date_range_df.withColumn("year", F.year("date"))
        date_range_df = date_range_df.withColumn("month", F.month("date"))
        date_range_df = date_range_df.withColumn("day", F.dayofmonth("date"))
        date_range_df = date_range_df.withColumn("week", F.weekofyear("date"))
        date_range_df = date_range_df.withColumn("quarter", F.quarter("date"))
        date_range_df = date_range_df.withColumn("day_of_week", F.dayofweek("date"))
        date_range_df = date_range_df.withColumn(
            "is_weekend", F.when(F.col("day_of_week").isin(6, 7), True).otherwise(False)
        )
        date_range_df = date_range_df.withColumn("superlake_dt", F.lit(superlake_dt))
        return date_range_df

    return dim_date, generate_dim_date

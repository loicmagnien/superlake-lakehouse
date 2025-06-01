import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from superlake.core import SuperDeltaTable, TableSaveMode, SchemaEvolution


def get_pipeline_objects_crm_contacts(super_spark, catalog_name, logger, managed, superlake_dt):
    # source schema
    source_crm_contacts_schema = T.StructType([
        T.StructField("contact_id", T.StringType(), False, {"description": "Unique ID of the contact"}),
        T.StructField("customer_id", T.StringType(), True, {"description": "Unique ID of the customer"}),
        T.StructField("first_name", T.StringType(), True, {"description": "First name of the contact"}),
        T.StructField("last_name", T.StringType(), True, {"description": "Last name of the contact"}),
        T.StructField("email", T.StringType(), True, {"description": "Email of the contact"}),
        T.StructField("phone", T.StringType(), True, {"description": "Phone number of the contact"}),
        T.StructField("address", T.StringType(), True, {"description": "Address of the contact"}),
        T.StructField("city", T.StringType(), True, {"description": "City of the contact"}),
        T.StructField("state", T.StringType(), True, {"description": "State of the contact"}),
        T.StructField("zip_code", T.StringType(), True, {"description": "Zip code of the contact"}),
        T.StructField("country", T.StringType(), True, {"description": "Country of the contact"}),
        T.StructField("job_title", T.StringType(), True, {"description": "Job title of the contact"}),
        T.StructField("created_at", T.TimestampType(), True, {"description": "Timestamp of the contact creation"}),
        T.StructField("updated_at", T.TimestampType(), True, {"description": "Timestamp of the contact update"}),
    ])

    # add superlake_dt to the schema for bronze and silver tables
    crm_contacts_schema = source_crm_contacts_schema.add(
        T.StructField("superlake_dt", T.TimestampType(), True, {"description": "Timestamp of the data ingestion"})
    )

    # bronze table
    bronze_crm_contacts = SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="01_bronze",
        table_name="crm_contacts",
        table_schema=crm_contacts_schema,
        table_save_mode=TableSaveMode.Append,
        primary_keys=["contact_id"],
        partition_cols=["superlake_dt"],
        pruning_partition_cols=True,
        pruning_primary_keys=False,
        optimize_table=False,
        optimize_zorder_cols=[],
        optimize_target_file_size=100000000,
        compression_codec="snappy",
        schema_evolution_option=SchemaEvolution.Merge,
        logger=logger,
        managed=managed
    )

    # silver table
    silver_crm_contacts = SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="02_silver",
        table_name="crm_contacts",
        table_schema=crm_contacts_schema,
        table_save_mode=TableSaveMode.Merge,
        primary_keys=["contact_id"],
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
        managed=managed
    )

    # cdc function
    def crm_contacts_cdc(spark):
        # Generate a static set of 10 contacts with customer_id from CUST1 to CUST10
        if silver_crm_contacts.table_exists(spark):
            contact_id_max = (
                silver_crm_contacts.read()
                .select(F.max(F.col("contact_id").cast("int")))
                .collect()[0][0]
            )
            if contact_id_max is None:
                contact_id_max = 1
            else:
                contact_id_max += 1
        else:
            contact_id_max = 1

        # Prepare static data
        data = []
        for i in range(10):
            contact_id = str(i)
            customer_id = f"CUST{i+1}"
            data.append((
                contact_id,
                customer_id,
                f"FirstName{contact_id}",
                f"LastName{contact_id}",
                f"contact{contact_id}@example.com",
                f"+1-555-01{str(i+1).zfill(2)}",
                f"Address {contact_id}",
                ["New York", "Chicago", "San Francisco"][i % 3],
                ["NY", "IL", "CA"][i % 3],
                f"100{str(i+1).zfill(2)}",
                "USA",
                "Manager" if i % 2 == 0 else "Sales Rep",
            ))
        columns = [
            "contact_id", "customer_id", "first_name", "last_name", "email", "phone", "address",
            "city", "state", "zip_code", "country", "job_title"
        ]
        df = spark.createDataFrame(data, columns)
        df = df.withColumn("created_at", F.current_timestamp())
        df = df.withColumn("updated_at", F.current_timestamp())
        return df

    # tra function
    def crm_contacts_transform(df: DataFrame):
        return df

    # del function
    def crm_contacts_delete(spark):
        return spark.createDataFrame([], source_crm_contacts_schema)

    return (
        bronze_crm_contacts,
        silver_crm_contacts,
        crm_contacts_cdc,
        crm_contacts_transform,
        None
    )

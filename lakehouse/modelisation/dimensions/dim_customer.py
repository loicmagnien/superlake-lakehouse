import pyspark.sql.types as T
from superlake.core import SuperDeltaTable, TableSaveMode, SchemaEvolution
from superlake.utils.modeling import SuperModeler
from lakehouse.ingestion.erp.erp_sales_transactions import get_pipeline_objects_erp_sales_transactions
from lakehouse.ingestion.crm.crm_contacts import get_pipeline_objects_crm_contacts
from pyspark.sql import functions as F


def get_model_dim_customer(super_spark, catalog_name, logger, managed, superlake_dt):
    dim_customer = SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=catalog_name,
        schema_name="03_gold",
        table_name="dim_customer",
        table_schema=T.StructType([
            T.StructField("customer_key", T.StringType(), False, {"description": "Unique key of the customer"}),
            T.StructField("customer_id", T.StringType(), False, {"description": "Unique ID of the customer"}),
            T.StructField("customer_name", T.StringType(), False, {"description": "Name of the customer"}),
            T.StructField("source_superlake_dt", T.TimestampType(), False, {"description": "Source ingestion timestamp"}),
            T.StructField("superlake_dt", T.TimestampType(), False, {"description": "Ingestion timestamp"}),
        ]),
        table_save_mode=TableSaveMode.MergeSCD,
        primary_keys=["customer_key"],
        scd_change_cols=["customer_id", "customer_name"],
        partition_cols=[],
        schema_evolution_option=SchemaEvolution.Merge,
        pruning_partition_cols=True,
        pruning_primary_keys=False,
        optimize_table=True,
        logger=logger,
        managed=managed,
        table_description=(
            "Dimension table for customers. Each row represents a unique customer, including their ID and name, "
            "and is used to join sales facts to customer details for customer-centric analysis."
        )
    )

    def generate_dim_customer(super_spark, catalog_name, logger, managed, superlake_dt):
        # Get silver tables
        (_, silver_erp_sales_transactions, _, _, _) = get_pipeline_objects_erp_sales_transactions(
            super_spark, catalog_name, logger, managed, superlake_dt
        )
        (_, silver_crm_contacts, _, _, _) = get_pipeline_objects_crm_contacts(
            super_spark, catalog_name, logger, managed, superlake_dt
        )
        # generate the dimension from the silver_erp_sales_transactions table
        erp_dim_customer = SuperModeler.generate_dimension(
            super_spark=super_spark,
            superlake_dt=superlake_dt,
            source_table=silver_erp_sales_transactions,
            source_columns=["customer_id", "customer_name"],
            source_keys=["customer_id"],
            sink_table=dim_customer
        )
        # note: the generate_dimension has renamed the superlake_dt column to source_superlake_dt
        # keep the rows for the latest source_superlake_dt for each customer_id
        latest_erp_dim_customer = erp_dim_customer.groupBy("customer_id").agg(
            F.max("source_superlake_dt").alias("latest_source_superlake_dt")
        )
        erp_dim_customer = erp_dim_customer.alias("erp_dim_customer").join(
            latest_erp_dim_customer.alias("latest_erp_dim_customer"),
            (
                (F.col("erp_dim_customer.customer_id") ==
                 F.col("latest_erp_dim_customer.customer_id")) &
                (F.col("erp_dim_customer.source_superlake_dt") ==
                 F.col("latest_erp_dim_customer.latest_source_superlake_dt"))
            ),
            how="inner"
        )
        # Drop the duplicate customer_id column from the right side
        erp_dim_customer = erp_dim_customer.drop(F.col("latest_erp_dim_customer.customer_id"))
        erp_dim_customer = erp_dim_customer.drop(F.col("latest_erp_dim_customer.latest_source_superlake_dt"))
        # get the data from the crm_contacts table
        crm_contacts_data = silver_crm_contacts.read()
        # add crm_contacts information to the dimension
        dim_customer_df = erp_dim_customer.alias("erp_dim_customer").join(
            crm_contacts_data.alias("crm_contacts_data"),
            F.col("erp_dim_customer.customer_id") == F.col("crm_contacts_data.customer_id"),
            how="left"
        )
        # select the columns to keep
        dim_customer_df = dim_customer_df.select(
            # from erp_dim_customer
            "erp_dim_customer.customer_key",
            "erp_dim_customer.customer_id",
            "erp_dim_customer.customer_name",
            "erp_dim_customer.source_superlake_dt",
            "erp_dim_customer.superlake_dt",
            # from crm_contacts
            "crm_contacts_data.contact_id",
            "crm_contacts_data.first_name",
            "crm_contacts_data.last_name",
            "crm_contacts_data.email",
            "crm_contacts_data.phone",
            "crm_contacts_data.address",
            "crm_contacts_data.city",
            "crm_contacts_data.state",
            "crm_contacts_data.zip_code",
            "crm_contacts_data.country",
            "crm_contacts_data.job_title"
        )
        return dim_customer_df

    return dim_customer, generate_dim_customer

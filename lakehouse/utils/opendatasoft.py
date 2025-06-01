import requests
import time


def load_opendatasoft_dataset_to_spark(
    domain,
    dataset_id,
    spark,
    schema,
    where=None,
    select=None,
    order_by=None,
    batch_size=10,
    max_retries=3,
    retry_delay=2
):
    """
    Load a complete dataset from Opendatasoft Explore v2 API into a PySpark DataFrame, with pagination and retries.

    Parameters:
    - domain (str): The Opendatasoft domain (e.g., 'opendata.paris.fr')
    - dataset_id (str): The dataset ID (e.g., 'velib-disponibilite-en-temps-reel')
    - spark (SparkSession): The active SparkSession to use.
    - schema (StructType): The schema to use for the DataFrame.
    - where (str): Optional SQL-style filter
    - select (str): Optional comma-separated fields to select
    - order_by (str): Optional ordering clause
    - batch_size (int): Number of records per page (max 10000)
    - max_retries (int): Retry attempts on failure
    - retry_delay (int): Delay in seconds between retries

    Returns:
    - PySpark DataFrame with all paginated results.
    """

    base_url = f"https://{domain}/api/explore/v2.1/catalog/datasets/{dataset_id}/records"

    all_records = []
    offset = 0

    while True:
        params = {
            "limit": batch_size,
            "offset": offset
        }
        if where:
            params["where"] = where
        if select:
            params["select"] = select
        if order_by:
            params["order_by"] = order_by

        records = None
        # Error handling with retries
        for attempt in range(max_retries):
            try:
                response = requests.get(base_url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                records = data.get("results", [])
                break  # success, break retry loop
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise RuntimeError(f"Failed after {max_retries} attempts: {str(e)}")
        # After retry loop, check if records is empty
        if not records:
            break  # break the outer while loop
        all_records.extend(records)
        offset += batch_size

    # Return as Spark DataFrame
    return spark.createDataFrame(all_records, schema=schema)

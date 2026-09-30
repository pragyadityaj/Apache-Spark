# writer.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from google.cloud import storage
from google.cloud import bigquery

def write_to_staging(spark: SparkSession, df, project: str, dataset: str, staging_table: str, target_table: str):
    """
    Reads existing active records from BigQuery, filters out unchanged records,
    and writes ONLY modified or brand-new records to the staging table.
    """
    target_path = f"{project}.{dataset}.{target_table}"
    
    try:
        # Read current active records from BigQuery
        df_target_active = (
            spark.read.format("bigquery")
            .option("table", target_path)
            .load()
            .filter(F.col("is_current") == True)
        )

        # Join incoming data with active target records on primary key
        joined_df = df.alias("inc").join(
            df_target_active.alias("target"),
            on="product_id",
            how="left"
        )

        # Record has changed if it's brand new OR if any monitored column differs
        cols = ["name", "category", "price", "supplier", "status"]
        has_changed = (
        F.col("target.product_id").isNull() | 
        (~F.col("inc.name").eqNullSafe(F.col("target.name"))) |
        (~F.col("inc.category").eqNullSafe(F.col("target.category"))) |
        (~F.col("inc.price").eqNullSafe(F.col("target.price"))) |
        (~F.col("inc.supplier").eqNullSafe(F.col("target.supplier"))) |
        (~F.col("inc.status").eqNullSafe(F.col("target.status")))
        )

        # Filter down to only new or modified records
        df_to_stage = joined_df.filter(has_changed).select("inc.*")

    except Exception as e:
        # Fallback if target table doesn't exist yet on initial load
        print(f"Notice: Could not read target table '{target_path}'. Ingesting full batch. Details: {e}")
        df_to_stage = df

    # Overwrite the staging table with only changed/new records
    (
        df_to_stage.select(
            "product_id",
            "name",
            "category",
            "price",
            "supplier",
            "status",
            "effective_start_date",
            "effective_end_date",
            "is_current"
        )
        .write
        .format("bigquery")
        .option("table", f"{project}.{dataset}.{staging_table}")
        .option("temporaryGcsBucket", "spark-dst-gds")
        .option("intermediateFormat", "parquet")
        .mode("overwrite")
        .save()
    )


def merge_scd2_bq(
    spark: SparkSession,
    project: str,
    dataset: str,
    staging_table: str,
    target_table: str,
):
    """
    Expire old SCD2 rows and insert new/changed ones directly in BigQuery.
    """
    key  = "product_id"
    cols = ["name", "category", "price", "supplier", "status"]
    on   = f"T.{key} = S.{key} AND T.is_current"
    changes = " OR ".join(f"T.{c} <> S.{c}" for c in cols)

    # 1. Expire current active records where changes were detected
    merge_sql = f"""
    MERGE `{project}.{dataset}.{target_table}` AS T
    USING `{project}.{dataset}.{staging_table}` AS S
      ON {on}
    WHEN MATCHED AND ({changes}) THEN
      UPDATE SET
        T.is_current = FALSE,
        T.effective_end_date = S.effective_start_date 
    """

    client = bigquery.Client(project=project)
    job = client.query(merge_sql)
    job.result()  # Wait for merge completion

    # 2. Insert all staged records into target table
    insert_query = f"""INSERT INTO `{project}.{dataset}.{target_table}` SELECT * FROM `{project}.{dataset}.{staging_table}`"""
    insert_staging_job = client.query(insert_query)
    insert_staging_job.result()  # Wait for insert completion
    print(f"Merge completed: {job.job_id}")


# def archive_processed_csv(bucket_name: str, proc_date: str):
#     """
#     Move processed CSV file from input/ to archive/ directory in GCS.
#     """
#     client = storage.Client()
#     bucket = client.bucket(bucket_name)
#     src = bucket.blob(f"products/input/products_{proc_date}.csv")
#     dst_name = f"products/archive/products_{proc_date}.csv"
#     bucket.copy_blob(src, bucket, dst_name)
#     src.delete()
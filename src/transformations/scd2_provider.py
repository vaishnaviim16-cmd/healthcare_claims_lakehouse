from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col,
    coalesce,
    concat_ws,
    current_timestamp,
    lag,
    lit,
    row_number,
    sha2,
    to_date,
    to_timestamp,
    trim,
    upper,
    when,
    lead,
   
)

from src.common.utils import load_yaml, table_path_from_config


VALID_PROVIDER_STATUSES = ["ACTIVE", "INACTIVE"]


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def write_delta(df: DataFrame, path: str) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(path)
    )


def standardize_provider_history(bronze_df: DataFrame) -> DataFrame:
    df = bronze_df

    # contract_start_date
    if "contract_start_date" in df.columns:
        df = df.withColumn("contract_start_date", to_date(col("contract_start_date")))
    else:
        df = df.withColumn("contract_start_date", to_date(col("updated_at")))

    # contract_end_date
    if "contract_end_date" in df.columns:
        df = df.withColumn("contract_end_date", to_date(col("contract_end_date")))
    else:
        df = df.withColumn("contract_end_date", lit(None).cast("date"))

    # quality_score
    if "quality_score" in df.columns:
        df = df.withColumn("quality_score", col("quality_score").cast("double"))
    else:
        df = df.withColumn("quality_score", lit(None).cast("double"))

    # updated_at
    if "updated_at" in df.columns:
        df = df.withColumn("updated_at", to_timestamp(col("updated_at")))
    else:
        df = df.withColumn("updated_at", lit(None).cast("timestamp"))

    # ingestion_timestamp
    if "ingestion_timestamp" in df.columns:
        df = df.withColumn("ingestion_timestamp", to_timestamp(col("ingestion_timestamp")))
    else:
        df = df.withColumn("ingestion_timestamp", lit(None).cast("timestamp"))

    # provider_status
    if "provider_status" in df.columns:
        df = df.withColumn("provider_status", upper(trim(coalesce(col("provider_status"), lit("ACTIVE")))))
    else:
        df = df.withColumn("provider_status", lit("ACTIVE"))

    # network_flag
    if "network_flag" in df.columns:
        df = df.withColumn("network_flag", upper(trim(coalesce(col("network_flag"), lit("N")))))
    else:
        df = df.withColumn("network_flag", lit("N"))

    # source_delete_flag
    if "source_delete_flag" in df.columns:
        df = df.withColumn("source_delete_flag", upper(trim(coalesce(col("source_delete_flag"), lit("N")))))
    else:
        df = df.withColumn("source_delete_flag", lit("N"))

    # specialty
    if "specialty" not in df.columns:
        df = df.withColumn("specialty", lit("GENERAL"))

    # filters
    df = df.filter(col("provider_id").isNotNull())
    df = df.filter(col("specialty").isNotNull())
    df = df.filter(col("contract_start_date").isNotNull())
    df = df.filter(col("provider_status").isin(*VALID_PROVIDER_STATUSES))
    df = df.filter(col("network_flag").isin("Y", "N"))

    return df


def build_dim_provider_scd2(bronze_provider_df: DataFrame) -> DataFrame:
    df = standardize_provider_history(bronze_provider_df)

    if "enterprise_provider_id" not in df.columns:
        df = df.withColumn("enterprise_provider_id", col("provider_id"))
        
    if "npi" not in df.columns:
        df = df.withColumn("npi", lit(None).cast("string"))

    if "provider_type" not in df.columns:
        df = df.withColumn("provider_type", lit("HOSPITAL"))

    if "taxonomy_code" not in df.columns:
        df = df.withColumn("taxonomy_code", lit(None).cast("string"))
        
    if "hospital_affiliation" not in df.columns:
        df = df.withColumn("hospital_affiliation", lit(None).cast("string"))

    if "address_line1" not in df.columns:
        if "address" in df.columns:
            df = df.withColumn("address_line1", col("address"))
        else:
            df = df.withColumn("address_line1", lit(None).cast("string"))

    if "zip_code" not in df.columns:
        df = df.withColumn("zip_code", lit(None).cast("string"))

    if "country" not in df.columns:
        df = df.withColumn("country", lit("INDIA"))

    if "phone" not in df.columns:
        df = df.withColumn("phone", lit(None).cast("string"))

    if "email" not in df.columns:
        df = df.withColumn("email", lit(None).cast("string"))

    df = df.dropDuplicates([
        "provider_id",
        "updated_at",
        "specialty",
        "network_flag",
        "address_line1",
        "city",
        "state",
        "provider_status",
        "source_delete_flag",
    ])

    tracked_cols = [
        coalesce(col("specialty").cast("string"), lit("NULL")),
        coalesce(col("network_flag").cast("string"), lit("NULL")),
        coalesce(col("address_line1").cast("string"), lit("NULL")),
        coalesce(col("city").cast("string"), lit("NULL")),
        coalesce(col("state").cast("string"), lit("NULL")),
        coalesce(col("provider_status").cast("string"), lit("NULL")),
        coalesce(col("source_delete_flag").cast("string"), lit("NULL")),
    ]

    df = df.withColumn("change_hash", sha2(concat_ws("||", *tracked_cols), 256))

    provider_window = Window.partitionBy("provider_id").orderBy(
        col("updated_at").asc_nulls_last(),
        col("ingestion_timestamp").asc_nulls_last(),
    )

    df = df.withColumn("prev_change_hash", lag("change_hash").over(provider_window))
    df = df.filter(
        col("prev_change_hash").isNull() | (col("change_hash") != col("prev_change_hash"))
    )

    df = df.withColumn("effective_from", col("updated_at"))
    df = df.withColumn("next_effective_from", lead("updated_at").over(provider_window))
    df = df.withColumn("effective_to", col("next_effective_from"))
    df = df.withColumn("is_current", when(col("next_effective_from").isNull(), lit("Y")).otherwise(lit("N")))
    df = df.withColumn("is_deleted", when(col("source_delete_flag") == "Y", lit("Y")).otherwise(lit("N")))
    df = df.withColumn("dw_created_at", current_timestamp())

    surrogate_window = Window.orderBy(
        col("provider_id").asc_nulls_last(),
        col("effective_from").asc_nulls_last(),
    )

    df = df.withColumn("provider_scd2_sk", row_number().over(surrogate_window))

    return df.select(
        "provider_scd2_sk",
        "provider_id",
        "enterprise_provider_id",
        "npi",
        "provider_name",
        "provider_type",
        "specialty",
        "taxonomy_code",
        "network_flag",
        "hospital_affiliation",
        "address_line1",
        "city",
        "state",
        "zip_code",
        "country",
        "phone",
        "email",
        "contract_start_date",
        "contract_end_date",
        "provider_status",
        "quality_score",
        "source_delete_flag",
        "is_deleted",
        "effective_from",
        "effective_to",
        "is_current",
        "updated_at",
        "dw_created_at",
    )


def run_provider_scd2(
    spark: SparkSession,
    config_path: str = "configs/app_config.yaml",
) -> dict:
    config = load_yaml(config_path)

    bronze_provider_path = str(table_path_from_config(config, "bronze_providers"))
    dim_provider_scd2_path = str(table_path_from_config(config, "dim_provider_scd2"))

    bronze_provider_df = read_delta(spark, bronze_provider_path)
    dim_provider_scd2_df = build_dim_provider_scd2(bronze_provider_df)

    write_delta(dim_provider_scd2_df, dim_provider_scd2_path)

    return {
        "dim_provider_scd2_rows": dim_provider_scd2_df.count(),
        "dim_provider_scd2_current_rows": dim_provider_scd2_df.filter(col("is_current") == "Y").count(),
    }
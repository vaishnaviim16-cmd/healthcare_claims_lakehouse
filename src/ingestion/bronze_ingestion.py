from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    concat_ws,
    current_timestamp,
    input_file_name,
    lit,
    regexp_extract,
    sha2,
)

from src.common.utils import (
    landing_dataset_path,
    load_yaml,
    table_path_from_config,
)


def add_bronze_metadata(df: DataFrame, dataset_name: str) -> DataFrame:
    source_cols = [coalesce(col(c).cast("string"), lit("NULL")) for c in df.columns]

    return (
        df.withColumn("source_file_path", input_file_name())
        .withColumn("source_file_name", regexp_extract(col("source_file_path"), r"([^/\\]+$)", 1))
        .withColumn("bronze_batch_id", regexp_extract(col("source_file_path"), r"(batch_\d+)", 1))
        .withColumn("bronze_dataset", lit(dataset_name))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("record_hash", sha2(concat_ws("||", *source_cols), 256))
    )


def read_claims(spark: SparkSession, path_pattern: str) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path_pattern)
    )


def read_members(spark: SparkSession, path_pattern: str) -> DataFrame:
    return spark.read.json(path_pattern)


def read_providers(spark: SparkSession, path_pattern: str) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path_pattern)
    )


def read_payments(spark: SparkSession, path_pattern: str) -> DataFrame:
    return spark.read.parquet(path_pattern)


def write_delta(df: DataFrame, target_path: str, mode: str = "overwrite") -> None:
    (
        df.write
        .format("delta")
        .mode(mode)
        .save(target_path)
    )


def ingest_claims_to_bronze(spark: SparkSession, config: dict, mode: str = "overwrite") -> int:
    landing_dir = landing_dataset_path(config, "claims")
    path_pattern = str(landing_dir / "*" / "*.csv")

    df = read_claims(spark, path_pattern)
    df = add_bronze_metadata(df, "claims")

    target_path = str(table_path_from_config(config, "bronze_claims"))
    write_delta(df, target_path, mode=mode)
    return df.count()


def ingest_members_to_bronze(spark: SparkSession, config: dict, mode: str = "overwrite") -> int:
    landing_dir = landing_dataset_path(config, "members")
    path_pattern = str(landing_dir / "*" / "*.json")

    df = read_members(spark, path_pattern)
    df = add_bronze_metadata(df, "members")

    target_path = str(table_path_from_config(config, "bronze_members"))
    write_delta(df, target_path, mode=mode)
    return df.count()


def ingest_providers_to_bronze(spark: SparkSession, config: dict, mode: str = "overwrite") -> int:
    landing_dir = landing_dataset_path(config, "providers")
    path_pattern = str(landing_dir / "*" / "*.csv")

    df = read_providers(spark, path_pattern)
    df = add_bronze_metadata(df, "providers")

    target_path = str(table_path_from_config(config, "bronze_providers"))
    write_delta(df, target_path, mode=mode)
    return df.count()


def ingest_payments_to_bronze(spark: SparkSession, config: dict, mode: str = "overwrite") -> int:
    landing_dir = landing_dataset_path(config, "payments")
    path_pattern = str(landing_dir / "*" / "*.parquet")

    df = read_payments(spark, path_pattern)
    df = add_bronze_metadata(df, "payments")

    target_path = str(table_path_from_config(config, "bronze_payments"))
    write_delta(df, target_path, mode=mode)
    return df.count()


def run_bronze_ingestion(
    spark: SparkSession,
    config_path: str = "configs/app_config.yaml",
    mode: str = "overwrite",
) -> dict:
    config = load_yaml(config_path)

    results = {
        "bronze_claims_rows": ingest_claims_to_bronze(spark, config, mode=mode),
        "bronze_members_rows": ingest_members_to_bronze(spark, config, mode=mode),
        "bronze_providers_rows": ingest_providers_to_bronze(spark, config, mode=mode),
        "bronze_payments_rows": ingest_payments_to_bronze(spark, config, mode=mode),
    }
    return results


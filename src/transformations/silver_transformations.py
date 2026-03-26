from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    lit,
    to_timestamp,
    trim,
    expr,
    upper,
)

from src.common.utils import load_yaml, table_path_from_config
from src.quality.rules import build_reject_reason, keep_latest_record, split_valid_invalid


VALID_CLAIM_STATUSES = ["APPROVED", "DENIED", "PENDING"]
VALID_MEMBER_STATUSES = ["ACTIVE", "INACTIVE", "SUSPENDED"]
VALID_PROVIDER_STATUSES = ["ACTIVE", "INACTIVE"]
VALID_PAYMENT_STATUSES = ["PAID", "PARTIAL", "ON_HOLD"]


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def write_delta(df: DataFrame, path: str) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(path)
    )


def ensure_column(df: DataFrame, col_name: str, data_type: str, default_value=None) -> DataFrame:
    if col_name in df.columns:
        return df.withColumn(col_name, col(col_name).cast(data_type))
    return df.withColumn(col_name, lit(default_value).cast(data_type))


def ensure_date_column(df: DataFrame, col_name: str, default_value=None) -> DataFrame:
    if col_name in df.columns:
        return df.withColumn(
            col_name,
            expr(f"try_cast(`{col_name}` as date)")
        )
    return df.withColumn(col_name, lit(default_value).cast("date"))


def ensure_timestamp_column(df: DataFrame, col_name: str, default_value=None) -> DataFrame:
    if col_name in df.columns:
        return df.withColumn(col_name, to_timestamp(col(col_name)))
    return df.withColumn(col_name, lit(default_value).cast("timestamp"))


def ensure_string_column(df: DataFrame, col_name: str, default_value=None) -> DataFrame:
    if col_name in df.columns:
        return df.withColumn(col_name, col(col_name).cast("string"))
    return df.withColumn(col_name, lit(default_value).cast("string"))


def ensure_flag_column(df: DataFrame, col_name: str, default_value="N") -> DataFrame:
    if col_name in df.columns:
        return df.withColumn(col_name, upper(trim(coalesce(col(col_name), lit(default_value)))))
    return df.withColumn(col_name, lit(default_value))


def transform_claims(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = keep_latest_record(bronze_df, ["claim_id", "claim_line_id"])

    df = ensure_column(df, "claim_amount", "double")
    df = ensure_column(df, "approved_amount", "double")
    df = ensure_column(df, "paid_amount", "double")
    df = ensure_column(df, "copay_amount", "double")
    df = ensure_column(df, "coinsurance_amount", "double")
    df = ensure_column(df, "deductible_amount", "double")

    df = ensure_date_column(df, "service_date")
    df = ensure_date_column(df, "received_date")
    df = ensure_date_column(df, "service_from_date")
    df = ensure_date_column(df, "service_to_date")
    df = ensure_date_column(df, "processed_date")
    df = ensure_date_column(df, "admit_date")
    df = ensure_date_column(df, "discharge_date")

    df = ensure_timestamp_column(df, "updated_at")
    df = ensure_timestamp_column(df, "ingestion_timestamp")

    if "claim_status" in df.columns:
        df = df.withColumn("claim_status", upper(trim(col("claim_status"))))
    else:
        df = df.withColumn("claim_status", lit(None).cast("string"))

    df = ensure_flag_column(df, "source_delete_flag", "N")
    df = ensure_flag_column(df, "network_flag", "N")
    df = ensure_flag_column(df, "is_emergency", "N")
    df = ensure_column(df, "claim_version", "int", 1)

    rules = [
        (col("claim_id").isNotNull(), "claim_id is null"),
        (col("claim_line_id").isNotNull(), "claim_line_id is null"),
        (col("member_id").isNotNull(), "member_id is null"),
        (col("provider_id").isNotNull(), "provider_id is null"),
        (col("claim_amount").isNotNull(), "claim_amount is null"),
        (col("claim_amount") >= 0, "claim_amount is negative"),
        (col("service_date").isNotNull(), "service_date is invalid"),
        (col("claim_status").isin(*VALID_CLAIM_STATUSES), "claim_status is invalid"),
    ]

    df = df.withColumn("reject_reason", build_reject_reason(rules))
    valid_df, reject_df = split_valid_invalid(df)

    valid_df = valid_df.filter(col("source_delete_flag") != "Y")
    return valid_df, reject_df


def transform_members(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = keep_latest_record(bronze_df, ["member_id"])

    df = ensure_date_column(df, "dob")
    df = ensure_date_column(df, "effective_date")
    df = ensure_date_column(df, "termination_date")
    df = ensure_column(df, "risk_score", "double")
    df = ensure_timestamp_column(df, "updated_at")
    df = ensure_timestamp_column(df, "ingestion_timestamp")
    df = ensure_flag_column(df, "source_delete_flag", "N")

    if "member_status" in df.columns:
        df = df.withColumn("member_status", upper(trim(col("member_status"))))
    else:
        df = df.withColumn("member_status", lit(None).cast("string"))
    
    if "payer_id" not in bronze_df.columns:
        df = df.withColumn("payer_id", lit("PAYER_UNKNOWN"))

    if "plan_id" not in bronze_df.columns:
        df = df.withColumn("plan_id", lit("PLAN_UNKNOWN"))

    if "effective_date" not in bronze_df.columns:
        df = df.withColumn("effective_date", expr("try_cast(updated_at as date)"))

    if "member_status" not in bronze_df.columns:
        df = df.withColumn("member_status", lit("ACTIVE"))
    else:
        df = df.withColumn(
        "member_status",
        upper(trim(coalesce(col("member_status"), lit("ACTIVE"))))
    )
        

    df = ensure_string_column(df, "payer_id")
    df = ensure_string_column(df, "plan_id")
    
    
    rules = [
    (col("member_id").isNotNull(), "member_id is null"),
    ]

    df = df.withColumn("reject_reason", build_reject_reason(rules))
    valid_df, reject_df = split_valid_invalid(df)

    valid_df = valid_df.filter(col("source_delete_flag") != "Y")
    return valid_df, reject_df


def transform_providers(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = keep_latest_record(bronze_df, ["provider_id"])

    df = ensure_date_column(df, "contract_start_date")
    df = ensure_date_column(df, "contract_end_date")
    df = ensure_column(df, "quality_score", "double")
    df = ensure_timestamp_column(df, "updated_at")
    df = ensure_timestamp_column(df, "ingestion_timestamp")
    df = ensure_flag_column(df, "network_flag", "N")
    df = ensure_flag_column(df, "source_delete_flag", "N")

    if "provider_status" in df.columns:
        df = df.withColumn("provider_status", upper(trim(col("provider_status"))))
    else:
        df = df.withColumn("provider_status", lit(None).cast("string"))

    df = ensure_string_column(df, "specialty")

    if "specialty" not in bronze_df.columns:
        df = df.withColumn("specialty", lit("GENERAL"))

    if "contract_start_date" not in bronze_df.columns:
        df = df.withColumn("contract_start_date", expr("try_cast(updated_at as date)"))

    if "provider_status" not in bronze_df.columns:
        df = df.withColumn("provider_status", lit("ACTIVE"))
    else:
        df = df.withColumn(
        "provider_status",
        upper(trim(coalesce(col("provider_status"), lit("ACTIVE"))))
    )

    rules = [
    (col("provider_id").isNotNull(), "provider_id is null"),
    ]

    df = df.withColumn("reject_reason", build_reject_reason(rules))
    valid_df, reject_df = split_valid_invalid(df)

    valid_df = valid_df.filter(col("source_delete_flag") != "Y")
    return valid_df, reject_df


def transform_payments(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = keep_latest_record(bronze_df, ["payment_id"])

    
    df = ensure_column(df, "adjustment_amount", "double")
    df = ensure_column(df, "interest_amount", "double")
    df = ensure_column(df, "withhold_amount", "double")
    df = ensure_date_column(df, "payment_date")
    df = ensure_timestamp_column(df, "updated_at")
    df = ensure_timestamp_column(df, "ingestion_timestamp")
    df = ensure_flag_column(df, "source_delete_flag", "N")

    if "payment_amount" not in bronze_df.columns and "paid_amount" in bronze_df.columns:
        df = df.withColumn("payment_amount", col("paid_amount").cast("double"))
    else:
        df = ensure_column(df, "payment_amount", "double")

    if "payment_status" in df.columns:
        df = df.withColumn("payment_status", upper(trim(col("payment_status"))))
    else:
        df = df.withColumn("payment_status", lit(None).cast("string"))

    rules = [
    (col("payment_id").isNotNull(), "payment_id is null"),
    (col("claim_id").isNotNull(), "claim_id is null"),
    (col("payment_amount").isNotNull(), "payment_amount is null"),
    (col("payment_amount") >= 0, "payment_amount is negative"),
    ]

    df = df.withColumn("reject_reason", build_reject_reason(rules))
    valid_df, reject_df = split_valid_invalid(df)

    valid_df = valid_df.filter(col("source_delete_flag") != "Y")
    return valid_df, reject_df


def run_silver_pipeline(
    spark: SparkSession,
    config_path: str = "configs/app_config.yaml",
) -> dict:
    config = load_yaml(config_path)

    bronze_claims_path = str(table_path_from_config(config, "bronze_claims"))
    bronze_members_path = str(table_path_from_config(config, "bronze_members"))
    bronze_providers_path = str(table_path_from_config(config, "bronze_providers"))
    bronze_payments_path = str(table_path_from_config(config, "bronze_payments"))

    silver_claims_path = str(table_path_from_config(config, "silver_claims"))
    silver_claims_rejects_path = str(table_path_from_config(config, "silver_claims_rejects"))

    silver_members_path = str(table_path_from_config(config, "silver_members"))
    silver_members_rejects_path = str(table_path_from_config(config, "silver_members_rejects"))

    silver_providers_path = str(table_path_from_config(config, "silver_providers"))
    silver_providers_rejects_path = str(table_path_from_config(config, "silver_providers_rejects"))

    silver_payments_path = str(table_path_from_config(config, "silver_payments"))
    silver_payments_rejects_path = str(table_path_from_config(config, "silver_payments_rejects"))

    bronze_claims_df = read_delta(spark, bronze_claims_path)
    bronze_members_df = read_delta(spark, bronze_members_path)
    bronze_providers_df = read_delta(spark, bronze_providers_path)
    bronze_payments_df = read_delta(spark, bronze_payments_path)

    silver_claims_df, silver_claims_rejects_df = transform_claims(bronze_claims_df)
    silver_members_df, silver_members_rejects_df = transform_members(bronze_members_df)
    silver_providers_df, silver_providers_rejects_df = transform_providers(bronze_providers_df)
    silver_payments_df, silver_payments_rejects_df = transform_payments(bronze_payments_df)

    write_delta(silver_claims_df, silver_claims_path)
    write_delta(silver_claims_rejects_df, silver_claims_rejects_path)

    write_delta(silver_members_df, silver_members_path)
    write_delta(silver_members_rejects_df, silver_members_rejects_path)

    write_delta(silver_providers_df, silver_providers_path)
    write_delta(silver_providers_rejects_df, silver_providers_rejects_path)

    write_delta(silver_payments_df, silver_payments_path)
    write_delta(silver_payments_rejects_df, silver_payments_rejects_path)

    return {
        "silver_claims_rows": silver_claims_df.count(),
        "silver_claims_reject_rows": silver_claims_rejects_df.count(),
        "silver_members_rows": silver_members_df.count(),
        "silver_members_reject_rows": silver_members_rejects_df.count(),
        "silver_providers_rows": silver_providers_df.count(),
        "silver_providers_reject_rows": silver_providers_rejects_df.count(),
        "silver_payments_rows": silver_payments_df.count(),
        "silver_payments_reject_rows": silver_payments_rejects_df.count(),
    }
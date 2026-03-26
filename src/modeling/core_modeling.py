from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    lit,
    row_number,
    split,
    trim,
    when,
)

from src.common.utils import load_yaml, table_path_from_config


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def write_delta(df: DataFrame, path: str) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(path)
    )


def add_surrogate_key(df: DataFrame, key_cols: list[str], surrogate_key_name: str) -> DataFrame:
    window_spec = Window.orderBy(*[col(c).asc_nulls_last() for c in key_cols])
    return df.withColumn(surrogate_key_name, row_number().over(window_spec))


def build_dim_member_current(silver_members_df: DataFrame) -> DataFrame:
    df = silver_members_df

    if "full_name" in df.columns:
        df = df.withColumn("first_name", split(trim(col("full_name")), " ").getItem(0))
        df = df.withColumn(
            "last_name",
            when(
                split(trim(col("full_name")), " ").getItem(1).isNotNull(),
                split(trim(col("full_name")), " ").getItem(1)
            ).otherwise(lit(None))
        )
    else:
        df = df.withColumn("first_name", lit(None).cast("string"))
        df = df.withColumn("last_name", lit(None).cast("string"))

    if "enterprise_member_id" not in df.columns:
        df = df.withColumn("enterprise_member_id", col("member_id"))

    if "phone" not in df.columns:
        df = df.withColumn("phone", lit(None).cast("string"))

    if "email" not in df.columns:
        df = df.withColumn("email", lit(None).cast("string"))

    if "address_line1" not in df.columns:
        df = df.withColumn("address_line1", lit(None).cast("string"))

    if "zip_code" not in df.columns:
        df = df.withColumn("zip_code", lit(None).cast("string"))

    if "country" not in df.columns:
        df = df.withColumn("country", lit("INDIA"))

    if "plan_name" not in df.columns:
        df = df.withColumn("plan_name", lit(None).cast("string"))

    if "primary_provider_id" not in df.columns:
        df = df.withColumn("primary_provider_id", lit(None).cast("string"))

    df = df.select(
        col("member_id"),
        col("enterprise_member_id"),
        col("first_name"),
        col("last_name"),
        col("gender"),
        col("dob"),
        col("phone"),
        col("email"),
        col("address_line1"),
        col("city"),
        col("state"),
        col("zip_code"),
        col("country"),
        col("plan_id"),
        col("plan_name"),
        col("payer_id"),
        col("member_status"),
        col("effective_date"),
        col("termination_date"),
        col("primary_provider_id"),
        col("risk_score"),
        col("updated_at"),
    )

    df = add_surrogate_key(df, ["member_id"], "member_sk")
    return df


def build_dim_provider_current(silver_providers_df: DataFrame) -> DataFrame:
    df = silver_providers_df

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
        df = df.withColumn("address_line1", lit(None).cast("string"))

    if "zip_code" not in df.columns:
        df = df.withColumn("zip_code", lit(None).cast("string"))

    if "country" not in df.columns:
        df = df.withColumn("country", lit("INDIA"))

    if "phone" not in df.columns:
        df = df.withColumn("phone", lit(None).cast("string"))

    if "email" not in df.columns:
        df = df.withColumn("email", lit(None).cast("string"))

    df = df.select(
        col("provider_id"),
        col("enterprise_provider_id"),
        col("npi"),
        col("provider_name"),
        col("provider_type"),
        col("specialty"),
        col("taxonomy_code"),
        col("network_flag"),
        col("hospital_affiliation"),
        col("address_line1"),
        col("city"),
        col("state"),
        col("zip_code"),
        col("country"),
        col("phone"),
        col("email"),
        col("contract_start_date"),
        col("contract_end_date"),
        col("provider_status"),
        col("quality_score"),
        col("updated_at"),
    )

    df = add_surrogate_key(df, ["provider_id"], "provider_current_sk")
    return df


def build_fact_claim(
    silver_claims_df: DataFrame,
    dim_member_df: DataFrame,
    dim_provider_current_df: DataFrame,
) -> DataFrame:
    member_lookup = dim_member_df.select("member_id", "member_sk")
    provider_lookup = dim_provider_current_df.select("provider_id", "provider_current_sk")

    df = (
        silver_claims_df.alias("c")
        .join(member_lookup.alias("m"), col("c.member_id") == col("m.member_id"), "left")
        .join(provider_lookup.alias("p"), col("c.provider_id") == col("p.provider_id"), "left")
    )

    if "paid_amount" not in df.columns:
        df = df.withColumn("paid_amount", lit(None).cast("double"))

    if "claim_version" not in df.columns:
        df = df.withColumn("claim_version", lit(1).cast("int"))

    if "service_date" not in df.columns:
        df = df.withColumn("service_date", lit(None).cast("date"))

    if "received_date" not in df.columns:
        df = df.withColumn("received_date", lit(None).cast("date"))

    df = df.select(
        col("c.claim_id"),
        col("c.claim_line_id"),
        col("m.member_sk"),
        col("p.provider_current_sk"),
        col("c.member_id"),
        col("c.provider_id"),
        col("c.payer_id"),
        col("c.diagnosis_code"),
        col("c.procedure_code"),
        col("c.claim_amount"),
        col("c.approved_amount"),
        col("paid_amount"),
        col("c.claim_status"),
        col("service_date"),
        col("received_date"),
        col("claim_version"),
        col("c.updated_at"),
        col("c.record_hash"),
    ).withColumn(
        "claim_year_month",
        date_format(col("service_date"), "yyyy-MM")
    ).withColumn(
        "approved_flag",
        when(col("claim_status") == "APPROVED", lit(1)).otherwise(lit(0))
    ).withColumn(
        "denied_flag",
        when(col("claim_status") == "DENIED", lit(1)).otherwise(lit(0))
    ).withColumn(
        "pending_flag",
        when(col("claim_status") == "PENDING", lit(1)).otherwise(lit(0))
    ).withColumn(
        "dw_created_at",
        current_timestamp()
    )

    df = add_surrogate_key(df, ["claim_id", "claim_line_id"], "claim_sk")
    return df


def build_fact_payment(
    silver_payments_df: DataFrame,
    dim_member_df: DataFrame,
    dim_provider_current_df: DataFrame,
    fact_claim_df: DataFrame,
) -> DataFrame:
    df = silver_payments_df

    if "payment_amount" not in df.columns and "paid_amount" in df.columns:
        df = df.withColumn("payment_amount", col("paid_amount").cast("double"))

    if "payment_method" not in df.columns:
        df = df.withColumn("payment_method", lit(None).cast("string"))

    if "bank_reference_number" not in df.columns:
        df = df.withColumn("bank_reference_number", lit(None).cast("string"))

    if "check_number" not in df.columns:
        df = df.withColumn("check_number", lit(None).cast("string"))

    if "remittance_number" not in df.columns:
        df = df.withColumn("remittance_number", lit(None).cast("string"))

    if "currency_code" not in df.columns:
        df = df.withColumn("currency_code", lit("INR"))

    if "payment_run_id" not in df.columns:
        df = df.withColumn("payment_run_id", lit(None).cast("string"))

    if "claim_line_id" not in df.columns:
        df = df.withColumn("claim_line_id", lit(None).cast("string"))

    if "member_id" not in df.columns:
        df = df.withColumn("member_id", lit(None).cast("string"))

    if "provider_id" not in df.columns:
        df = df.withColumn("provider_id", lit(None).cast("string"))

    if "payer_id" not in df.columns:
        df = df.withColumn("payer_id", lit(None).cast("string"))

    member_lookup = dim_member_df.select("member_id", "member_sk")
    provider_lookup = dim_provider_current_df.select("provider_id", "provider_current_sk")
    claim_lookup = fact_claim_df.select("claim_id", "claim_line_id", "claim_sk")

    df = (
        df.alias("p")
        .join(member_lookup.alias("m"), col("p.member_id") == col("m.member_id"), "left")
        .join(provider_lookup.alias("pr"), col("p.provider_id") == col("pr.provider_id"), "left")
        .join(
            claim_lookup.alias("c"),
            (col("p.claim_id") == col("c.claim_id")) & (col("p.claim_line_id") == col("c.claim_line_id")),
            "left",
        )
        .select(
            col("p.payment_id"),
            col("c.claim_sk"),
            col("m.member_sk"),
            col("pr.provider_current_sk"),
            col("p.claim_id"),
            col("p.claim_line_id"),
            col("p.member_id"),
            col("p.provider_id"),
            col("p.payer_id"),
            col("p.payment_amount"),
            col("p.payment_date"),
            col("p.payment_method"),
            col("p.payment_status"),
            col("p.bank_reference_number"),
            col("p.check_number"),
            col("p.remittance_number"),
            col("p.adjustment_amount"),
            col("p.interest_amount"),
            col("p.withhold_amount"),
            col("p.currency_code"),
            col("p.payment_run_id"),
            col("p.updated_at"),
            col("p.record_hash"),
        )
        .withColumn("payment_year_month", date_format(col("payment_date"), "yyyy-MM"))
        .withColumn("paid_flag", when(col("payment_status") == "PAID", lit(1)).otherwise(lit(0)))
        .withColumn("partial_flag", when(col("payment_status") == "PARTIAL", lit(1)).otherwise(lit(0)))
        .withColumn("on_hold_flag", when(col("payment_status") == "ON_HOLD", lit(1)).otherwise(lit(0)))
        .withColumn("dw_created_at", current_timestamp())
    )

    df = add_surrogate_key(df, ["payment_id"], "payment_sk")
    return df


def run_core_modeling(
    spark: SparkSession,
    config_path: str = "configs/app_config.yaml",
) -> dict:
    config = load_yaml(config_path)

    silver_claims_path = str(table_path_from_config(config, "silver_claims"))
    silver_members_path = str(table_path_from_config(config, "silver_members"))
    silver_providers_path = str(table_path_from_config(config, "silver_providers"))
    silver_payments_path = str(table_path_from_config(config, "silver_payments"))

    dim_member_path = str(table_path_from_config(config, "dim_member_current"))
    dim_provider_current_path = str(table_path_from_config(config, "dim_provider_current"))
    fact_claim_path = str(table_path_from_config(config, "fact_claim"))
    fact_payment_path = str(table_path_from_config(config, "fact_payment"))

    silver_claims_df = read_delta(spark, silver_claims_path)
    silver_members_df = read_delta(spark, silver_members_path)
    silver_providers_df = read_delta(spark, silver_providers_path)
    silver_payments_df = read_delta(spark, silver_payments_path)

    dim_member_df = build_dim_member_current(silver_members_df)
    dim_provider_current_df = build_dim_provider_current(silver_providers_df)

    fact_claim_df = build_fact_claim(
        silver_claims_df=silver_claims_df,
        dim_member_df=dim_member_df,
        dim_provider_current_df=dim_provider_current_df,
    )

    fact_payment_df = build_fact_payment(
        silver_payments_df=silver_payments_df,
        dim_member_df=dim_member_df,
        dim_provider_current_df=dim_provider_current_df,
        fact_claim_df=fact_claim_df,
    )

    write_delta(dim_member_df, dim_member_path)
    write_delta(dim_provider_current_df, dim_provider_current_path)
    write_delta(fact_claim_df, fact_claim_path)
    write_delta(fact_payment_df, fact_payment_path)

    return {
        "dim_member_current_rows": dim_member_df.count(),
        "dim_provider_current_rows": dim_provider_current_df.count(),
        "fact_claim_rows": fact_claim_df.count(),
        "fact_payment_rows": fact_payment_df.count(),
    }
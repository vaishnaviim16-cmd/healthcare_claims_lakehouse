from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    current_timestamp,
    lit,
    coalesce,
    max as spark_max,
    min as spark_min,
    round,
    sum as spark_sum,
    when,
    datediff
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


def build_gold_claim_cost_by_month(fact_claim_df):
    return (
        fact_claim_df
        .groupBy("claim_year_month", "payer_id")
        .agg(
            countDistinct("claim_id").alias("total_claims"),
            round(spark_sum("claim_amount"), 2).alias("total_claim_amount"),
            round(spark_sum("approved_amount"), 2).alias("total_approved_amount"),
            round(spark_sum("paid_amount"), 2).alias("total_paid_amount"),
            round(avg("claim_amount"), 2).alias("avg_claim_amount")
        )
    )


def build_gold_denial_analysis(fact_claim_df: DataFrame, current_provider_df: DataFrame) -> DataFrame:
    provider_month_base = (
        fact_claim_df.groupBy("claim_year_month", "provider_id")
        .agg(
            countDistinct("claim_id").alias("total_claims"),
            spark_sum("denied_flag").alias("denied_claims"),
        )
    )

    denied_reason = (
        fact_claim_df
        .filter(col("claim_status") == "DENIED")
        .withColumn(
            "denial_reason_code",
            coalesce(col("procedure_code"), lit("UNKNOWN"))
        )
        .groupBy("claim_year_month", "provider_id", "denial_reason_code")
        .agg(countDistinct("claim_id").alias("denial_reason_claims"))
    )

    return (
        denied_reason
        .join(provider_month_base, ["claim_year_month", "provider_id"], "left")
        .join(current_provider_df, ["provider_id"], "left")
        .withColumn(
            "denial_rate_pct",
            when(col("total_claims") > 0,
                 round((col("denied_claims") / col("total_claims")) * lit(100), 2))
            .otherwise(lit(0.0))
        )
        .withColumn("dw_created_at", current_timestamp())
    )

def build_gold_provider_performance(
    fact_claim_df: DataFrame,
    fact_payment_df: DataFrame,
    current_provider_df: DataFrame,
) -> DataFrame:
    payment_agg = (
        fact_payment_df.groupBy("provider_id")
        .agg(
            round(spark_sum("payment_amount"), 2).alias("total_payment_amount"),
            countDistinct("payment_id").alias("total_payments"),
        )
    )

    claims_df = fact_claim_df

    if "emergency_flag" not in claims_df.columns:
        claims_df = claims_df.withColumn("emergency_flag", lit(0))

    if "claim_turnaround_days" not in claims_df.columns:
        claims_df = claims_df.withColumn(
            "claim_turnaround_days",
            datediff(col("received_date"), col("service_date"))
        )

    claims_agg = (
        claims_df.groupBy("provider_id")
        .agg(
            countDistinct("claim_id").alias("total_claims"),
            spark_sum("approved_flag").alias("approved_claims"),
            spark_sum("denied_flag").alias("denied_claims"),
            spark_sum("pending_flag").alias("pending_claims"),
            spark_sum("emergency_flag").alias("emergency_claims"),
            round(spark_sum("claim_amount"), 2).alias("total_claim_amount"),
            round(spark_sum("approved_amount"), 2).alias("total_approved_amount"),
            round(avg("claim_turnaround_days"), 2).alias("avg_turnaround_days"),
        )
    )

    return (
        claims_agg.join(payment_agg, ["provider_id"], "left")
        .join(current_provider_df, ["provider_id"], "left")
        .fillna({"total_payment_amount": 0.0, "total_payments": 0})
        .withColumn(
            "approval_rate_pct",
            when(
                col("total_claims") > 0,
                round((col("approved_claims") / col("total_claims")) * lit(100), 2)
            ).otherwise(lit(0.0))
        )
        .withColumn(
            "denial_rate_pct",
            when(
                col("total_claims") > 0,
                round((col("denied_claims") / col("total_claims")) * lit(100), 2)
            ).otherwise(lit(0.0))
        )
        .withColumn("dw_created_at", current_timestamp())
    )

def build_gold_member_utilization(
    fact_claim_df: DataFrame,
    fact_payment_df: DataFrame,
    current_member_df: DataFrame,
) -> DataFrame:

    df = fact_claim_df

    if "emergency_flag" not in df.columns:
        df = df.withColumn("emergency_flag", lit(0))

    if "claim_turnaround_days" not in df.columns:
        df = df.withColumn(
            "claim_turnaround_days",
            datediff(col("received_date"), col("service_date"))
        )

    claim_agg = (
        df.groupBy("member_id", "payer_id")
        .agg(
            countDistinct("claim_id").alias("total_claims"),
            spark_sum("approved_flag").alias("approved_claims"),
            spark_sum("denied_flag").alias("denied_claims"),
            spark_sum("emergency_flag").alias("emergency_claims"),
            round(spark_sum("claim_amount"), 2).alias("total_claim_amount"),
            round(spark_sum("approved_amount"), 2).alias("total_approved_amount"),
            round(avg("claim_turnaround_days"), 2).alias("avg_turnaround_days"),
        )
    )

    payment_agg = (
        fact_payment_df.groupBy("member_id")
        .agg(
            round(spark_sum("payment_amount"), 2).alias("total_payment_amount"),
            countDistinct("payment_id").alias("total_payments"),
        )
    )

    member_lookup = current_member_df.select(
        "member_id",
        "member_sk",
        "enterprise_member_id",
        "first_name",
        "last_name",
        "gender",
        "dob",
        "city",
        "state",
        "member_status"
    )

    return (
        claim_agg
        .join(payment_agg, ["member_id"], "left")
        .join(member_lookup, ["member_id"], "left")
        .fillna({"total_payment_amount": 0.0, "total_payments": 0})
        .withColumn("dw_created_at", current_timestamp())
    )

def build_gold_claim_turnaround_time(fact_claim_df: DataFrame) -> DataFrame:
    df = fact_claim_df

    if "claim_turnaround_days" not in df.columns:
        df = df.withColumn(
            "claim_turnaround_days",
            datediff(col("received_date"), col("service_date"))
        )

    return (
        df.groupBy("claim_year_month", "claim_status")
        .agg(
            countDistinct("claim_id").alias("total_claims"),
            round(avg("claim_turnaround_days"), 2).alias("avg_turnaround_days"),
            spark_min("claim_turnaround_days").alias("min_turnaround_days"),
            spark_max("claim_turnaround_days").alias("max_turnaround_days"),
        )
        .withColumn("dw_created_at", current_timestamp())
    )


def run_gold_marts(
    spark: SparkSession,
    config_path: str = "configs/app_config.yaml",
) -> dict:
    config = load_yaml(config_path)

    fact_claim_path = str(table_path_from_config(config, "fact_claim"))
    fact_payment_path = str(table_path_from_config(config, "fact_payment"))
    dim_member_path = str(table_path_from_config(config, "dim_member_current"))
    dim_provider_scd2_path = str(table_path_from_config(config, "dim_provider_scd2"))

    gold_claim_cost_by_month_path = str(table_path_from_config(config, "gold_claim_cost_by_month"))
    gold_denial_analysis_path = str(table_path_from_config(config, "gold_denial_analysis"))
    gold_provider_performance_path = str(table_path_from_config(config, "gold_provider_performance"))
    gold_member_utilization_path = str(table_path_from_config(config, "gold_member_utilization"))
    gold_claim_turnaround_time_path = str(table_path_from_config(config, "gold_claim_turnaround_time"))

    fact_claim_df = read_delta(spark, fact_claim_path)
    fact_payment_df = read_delta(spark, fact_payment_path)
    dim_member_df = read_delta(spark, dim_member_path)
    dim_provider_scd2_df = read_delta(spark, dim_provider_scd2_path)

    current_provider_df = (
        dim_provider_scd2_df
        .filter((col("is_current") == "Y") & (col("is_deleted") == "N"))
        .select(
            "provider_id",
            "provider_name",
            "provider_type",
            "specialty",
            "network_flag",
            "provider_status",
            "quality_score",
            "city",
            "state",
        )
    )

    gold_claim_cost_by_month_df = build_gold_claim_cost_by_month(fact_claim_df)
    gold_denial_analysis_df = build_gold_denial_analysis(fact_claim_df, current_provider_df)
    gold_provider_performance_df = build_gold_provider_performance(
        fact_claim_df, fact_payment_df, current_provider_df
    )
    gold_member_utilization_df = build_gold_member_utilization(
        fact_claim_df, fact_payment_df, dim_member_df
    )
    gold_claim_turnaround_time_df = build_gold_claim_turnaround_time(fact_claim_df)

    write_delta(gold_claim_cost_by_month_df, gold_claim_cost_by_month_path)
    write_delta(gold_denial_analysis_df, gold_denial_analysis_path)
    write_delta(gold_provider_performance_df, gold_provider_performance_path)
    write_delta(gold_member_utilization_df, gold_member_utilization_path)
    write_delta(gold_claim_turnaround_time_df, gold_claim_turnaround_time_path)

    return {
        "gold_claim_cost_by_month_rows": gold_claim_cost_by_month_df.count(),
        "gold_denial_analysis_rows": gold_denial_analysis_df.count(),
        "gold_provider_performance_rows": gold_provider_performance_df.count(),
        "gold_member_utilization_rows": gold_member_utilization_df.count(),
        "gold_claim_turnaround_time_rows": gold_claim_turnaround_time_df.count(),
    }
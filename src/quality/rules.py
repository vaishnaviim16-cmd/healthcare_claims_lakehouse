from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, lit, row_number, to_timestamp, when
from pyspark.sql.window import Window


def keep_latest_record(df: DataFrame, business_keys: list[str]) -> DataFrame:
    order_cols = []

    if "updated_at" in df.columns:
        order_cols.append(to_timestamp(col("updated_at")).desc())

    if "ingestion_timestamp" in df.columns:
        order_cols.append(col("ingestion_timestamp").desc())

    if not order_cols:
        raise ValueError(
            "keep_latest_record requires at least one ordering column: "
            "'updated_at' or 'ingestion_timestamp'"
        )

    window_spec = Window.partitionBy(*business_keys).orderBy(*order_cols)

    return (
        df.withColumn("_rn", row_number().over(window_spec))
          .filter(col("_rn") == 1)
          .drop("_rn")
    )


def build_reject_reason(rules: list[tuple]) -> object:
    expressions = [
        when(condition, lit(None)).otherwise(lit(message))
        for condition, message in rules
    ]
    return concat_ws(" | ", *expressions)


def split_valid_invalid(df: DataFrame, reject_reason_col: str = "reject_reason") -> tuple[DataFrame, DataFrame]:
    valid_df = df.filter((col(reject_reason_col).isNull()) | (col(reject_reason_col) == ""))
    invalid_df = df.filter((col(reject_reason_col).isNotNull()) & (col(reject_reason_col) != ""))
    return valid_df, invalid_df
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from src.common.spark_session import get_spark_session
from src.transformations.silver_transformations import run_silver_pipeline
from src.common.utils import load_yaml, table_path_from_config


def main():
    spark = get_spark_session()

    results = run_silver_pipeline(spark)
    print("Silver transformation completed successfully.\n")
    for k, v in results.items():
        print(f"{k}: {v:,}")

    config = load_yaml("configs/app_config.yaml")

    silver_members_rejects_path = str(table_path_from_config(config, "silver_members_rejects"))
    silver_providers_rejects_path = str(table_path_from_config(config, "silver_providers_rejects"))
    silver_payments_rejects_path = str(table_path_from_config(config, "silver_payments_rejects"))

    print("\n=== MEMBERS REJECT REASONS ===")
    spark.read.format("delta").load(silver_members_rejects_path) \
        .groupBy("reject_reason").count().show(100, False)

    print("\n=== PROVIDERS REJECT REASONS ===")
    spark.read.format("delta").load(silver_providers_rejects_path) \
        .groupBy("reject_reason").count().show(100, False)

    print("\n=== PAYMENTS REJECT REASONS ===")
    spark.read.format("delta").load(silver_payments_rejects_path) \
        .groupBy("reject_reason").count().show(100, False)

    spark.stop()


if __name__ == "__main__":
    main()
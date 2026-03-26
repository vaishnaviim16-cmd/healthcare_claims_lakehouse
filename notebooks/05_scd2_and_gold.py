from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from src.common.spark_session import get_spark_session
from src.transformations.scd2_provider import run_provider_scd2
from src.marts.gold_marts import run_gold_marts


def main():
    spark = get_spark_session()

    scd2_results = run_provider_scd2(
        spark=spark,
        config_path=str(PROJECT_ROOT / "configs" / "app_config.yaml"),
    )

    gold_results = run_gold_marts(
        spark=spark,
        config_path=str(PROJECT_ROOT / "configs" / "app_config.yaml"),
    )

    print("\nProvider SCD2 + Gold marts completed successfully.\n")

    for key, value in scd2_results.items():
        print(f"{key}: {value:,}")

    for key, value in gold_results.items():
        print(f"{key}: {value:,}")

    spark.stop()


if __name__ == "__main__":
    main()
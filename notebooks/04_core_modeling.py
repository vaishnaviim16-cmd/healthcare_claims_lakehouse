from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from src.common.spark_session import get_spark_session
from src.modeling.core_modeling import run_core_modeling


def main():
    spark = get_spark_session()

    results = run_core_modeling(
        spark=spark,
        config_path=str(PROJECT_ROOT / "configs" / "app_config.yaml"),
    )

    print("\nCore modeling completed successfully.\n")
    for key, value in results.items():
        print(f"{key}: {value:,}")

    spark.stop()


if __name__ == "__main__":
    main()
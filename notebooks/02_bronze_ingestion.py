from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from src.common.spark_session import get_spark_session
from src.ingestion.bronze_ingestion import run_bronze_ingestion


def main():
    spark = get_spark_session()

    results = run_bronze_ingestion(
        spark=spark,
        config_path=str(PROJECT_ROOT / "configs" / "app_config.yaml"),
        mode="overwrite"
    )

    

    print("\nBronze ingestion completed successfully.\n")
    for key, value in results.items():
        print(f"{key}: {value:,}")

    spark.stop()


if __name__ == "__main__":
    main()
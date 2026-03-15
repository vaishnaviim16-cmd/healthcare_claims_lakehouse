from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.common.utils import load_yaml, project_root, ensure_dir


def get_spark_session(config_path: str = "configs/app_config.yaml") -> SparkSession:
    root = project_root()
    config = load_yaml(str(root / config_path))

    warehouse_dir = root / "spark-warehouse"
    ensure_dir(warehouse_dir)

    builder = (
        SparkSession.builder
        .appName(config["project"]["app_name"])
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", str(config["runtime"]["shuffle_partitions"]))
        .config("spark.sql.session.timeZone", config["runtime"]["timezone"])
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
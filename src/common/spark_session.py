import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.common.utils import load_yaml, project_root, ensure_dir

HADOOP_HOME = r"C:\hadoop"
HADOOP_BIN = r"C:\hadoop\bin"

os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["hadoop.home.dir"] = HADOOP_HOME
os.environ["PATH"] = HADOOP_BIN + os.pathsep + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def get_spark_session(config_path: str = "configs/app_config.yaml") -> SparkSession:
    root = project_root()
    config = load_yaml(str(root / config_path))

    warehouse_dir = root / "spark-warehouse"
    local_tmp_dir = root / "tmp" / "spark-local"

    ensure_dir(warehouse_dir)
    ensure_dir(local_tmp_dir)

    builder = (
        SparkSession.builder
        .appName(config["project"]["app_name"])
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", config["runtime"]["timezone"])
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.local.dir", str(local_tmp_dir))
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.debug.maxToStringFields", "200")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
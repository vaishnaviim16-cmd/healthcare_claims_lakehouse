import sys
from pathlib import Path
import shutil
import time

project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

from pyspark.sql import Row
from src.common.spark_session import get_spark_session


def main():
    spark = None
    output_path = project_root / "data" / "smoke" / "delta_output"

    try:
        spark = get_spark_session()

        data = [
            Row(id=1, name="Vaishnavi"),
            Row(id=2, name="Amit"),
            Row(id=3, name="Neha"),
        ]

        df = spark.createDataFrame(data)

        if output_path.exists():
            shutil.rmtree(output_path, ignore_errors=True)

        df.write.format("delta").mode("overwrite").save(str(output_path))

        read_df = spark.read.format("delta").load(str(output_path))
        read_df.show()

        print("Delta smoke test completed successfully.")

    finally:
        if spark is not None:
            spark.catalog.clearCache()
            spark.stop()
            time.sleep(5)


if __name__ == "__main__":
    main()
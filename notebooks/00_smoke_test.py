from pathlib import Path
import shutil

from src.common.spark_session import get_spark_session
from src.common.utils import project_root, ensure_dir


def main():
    spark = get_spark_session()
    root = project_root()

    output_path = root / "data" / "bronze" / "smoke_test"
    if output_path.exists():
        shutil.rmtree(output_path)

    ensure_dir(output_path.parent)

    data = [
        (1, "Vaishnavi"),
        (2, "Amit"),
        (3, "Neha")
    ]

    df = spark.createDataFrame(data, ["id", "name"])

    df.write.format("delta").mode("overwrite").save(str(output_path))

    read_df = spark.read.format("delta").load(str(output_path))
    read_df.show()

    print("Delta smoke test completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()